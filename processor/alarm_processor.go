package processor

import (
	"alarm-service/db"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
)

const (
	STATUS_NORMAL = iota
	STATUS_WARN
	STATUS_ALARM
	STATUS_OFFLINE
	STATUS_ERROR
)

var StatusCN = map[int]string{
	STATUS_NORMAL:  "正常",
	STATUS_WARN:    "预警",
	STATUS_ALARM:   "报警",
	STATUS_OFFLINE: "下线",
	STATUS_ERROR:   "故障"}

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

func (w *Worker) parseAlarmData(monData *MonitorData) {

	rows, err := db.MySqlDB.Query(
		"SELECT metric_key, metric_status_key, metric_display_name, metric_type from device " +
			"LEFT JOIN device_model model ON device.device_model = model.name " +
			"LEFT JOIN metric_config mc ON model.name = mc.device_model ")

	if err != nil {
		fmt.Printf("parseAlarmData Error: %v", err)
		return
	}

	defer rows.Close()

	alarmDescList := make(map[string][]string)

	for rows.Next() {
		var metricKey, metricStatusKey, metricDisplayName, metricType string

		err := rows.Scan(&metricKey, &metricStatusKey, &metricDisplayName, &metricType)

		if err != nil {
			fmt.Printf("Error Scan %v \n", err)
			monData.AlarmDescription = nil
		}

		if vMetricStatus, ok := monData.Data[0][metricStatusKey]; ok && vMetricStatus != STATUS_NORMAL {

			if _, ok := alarmDescList[metricType]; !ok {
				alarmDescList[metricType] = make([]string, 0, 10)
			}

			alarmDescList[metricType] = append(alarmDescList[metricType],
				fmt.Sprintf("【%s】发生【%s】, 当前值:%v",
					metricDisplayName,
					StatusCN[vMetricStatus.(int)],
					monData.Data[0][metricKey]))
		}
	}

	for _, v := range alarmDescList {
		monData.AlarmDescription = append(monData.AlarmDescription, strings.Join(v, " /"))
	}

	fmt.Println(monData.AlarmDescription)

}

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

	_, err = db.MySqlDB.Exec(
		"UPDATE device SET device_status = ?, status_time = FROM_UNIXTIME(?) "+
			"WHERE device_id = ? and status_time < FROM_UNIXTIME(?)",
		data.DeviceStatus, unixTime/1000, data.DeviceId, unixTime/1000)

	if err != nil {
		fmt.Printf("Exec update error %v \n", err)
		return err
	}

	return nil
}

func (w *Worker) processData(data ...*MonitorData) {

	for _, dt := range data {

		if dt.DeviceStatus == STATUS_NORMAL {
			continue
		}

		w.parseAlarmData(dt)

		w.saveAlarmData(dt)

		w.updateDeviceStatus(dt)

		w.sendAlarmNotification(dt)

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
