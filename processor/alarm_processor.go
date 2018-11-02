package processor

import (
	"alarm-service/db"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
)

const (
	STATUS_NORMAL = iota
	STATUS_WARN
	STATUS_ALARM
	STATUS_OFFLINE
	STATUS_ERROR
)

var wg sync.WaitGroup

type Worker struct {
	workerId      int
	maxDataBuffer int
	bulkData      []interface{}
	signals       chan os.Signal
	monData       chan []*MonitorData
	error         chan error
}

type AlarmDataProcessor struct {
	alarmData chan []*MonitorData
	workers   []*Worker
	signal    chan os.Signal
}

func (w *Worker) flushData(collection string, data ...interface{}) {

	fmt.Printf("Worker %d Flushing %s Data \n", w.workerId, collection)

	s := db.GetSession()
	defer s.Close()
	c := s.DB("pilot").C(collection)

	if err := c.Insert(data...); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf(" %d %s Data Flushed \n", len(data), collection)

}

//func (w *Worker) saveAlarmData(monData *MonitorData) {
//
//	w.flushData("alarm_data", monData)
//}

func (w *Worker) saveAlarmData(monData *MonitorData) {
	w.flushData("alarm_data", monData)
}

func (w *Worker) sendAlarmNotification(data *MonitorData) {
	fmt.Println("Sending Alarm")
}

func (w *Worker) updateDeviceStatus(data *MonitorData) error {

	unixTime, err := strconv.Atoi(data.TimeStamp)

	if err != nil {
		fmt.Println("Atoi错误")
		return err
	}

	// 更新设备状态
	err = db.ExecuteUpdate(
		"UPDATE device SET device_status = ?, status_time = FROM_UNIXTIME(?) WHERE device_id = ? and status_time < FROM_UNIXTIME(?)",
		data.DeviceStatus, unixTime/1000, data.DeviceId, unixTime/1000)

	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (w *Worker) processData(data ...*MonitorData) {

	for _, dt := range data {

		if dt.DeviceStatus == STATUS_NORMAL {
			continue
		}

		w.sendAlarmNotification(dt)

		w.saveAlarmData(dt)

		w.updateDeviceStatus(dt)

	}
}

func (w *Worker) Process() {

	fmt.Printf("Worker %d start processing \n", w.workerId)

Loop:
	for {
		select {

		case dt, ok := <-w.monData:
			if ok {
				w.processData(dt...)
			}
		case <-w.signals:
			fmt.Printf("Worker %d Received Signal, Flushing...\n", w.workerId)
			w.flushData("metric_data", w.bulkData...)
			break Loop
		}
	}

	fmt.Printf("Worker %d Done\n", w.workerId)
	wg.Done()
}

func NewProcessor(maxWorker int, maxBuffer int) AlarmDataProcessor {

	p := AlarmDataProcessor{
		alarmData: make(chan []*MonitorData),
		signal:    make(chan os.Signal, 1),
	}

	signal.Notify(p.signal, os.Interrupt)

	for i := 1; i <= maxWorker; i++ {
		p.workers = append(p.workers,
			&Worker{
				workerId:      i,
				maxDataBuffer: maxBuffer,
				monData:       p.alarmData,
				signals:       make(chan os.Signal, 1)})
	}

	return p
}

func (processor *AlarmDataProcessor) AddData(data ...*MonitorData) {

	processor.alarmData <- data
}

func (processor *AlarmDataProcessor) Run() {

	for _, w := range processor.workers {
		wg.Add(1)
		go w.Process()
	}

	go func() {

		select {
		case sig := <-processor.signal:
			fmt.Println("Stopping Workers")
			for _, w := range processor.workers {
				w.signals <- sig
			}
			return
		}
	}()

}

func (processor *AlarmDataProcessor) Wait() {

	wg.Wait()
}
