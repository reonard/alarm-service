package processor

import (
	"alarm-service/db"
	. "alarm-service/lib"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"testing"
	"time"
)

func TestNewProcessor(t *testing.T) {

	InitCfg()

	s := db.InitMongoDB("127.0.0.1:27017")
	defer s.Close()

	m := db.InitMySQLDB(AppCfg.MySqlURL)
	defer m.Close()

	p := NewProcessor(1, 1)
	p.Run()

	p.AddData(&MonitorData{DeviceId: 1, TimeStamp: strconv.Itoa(int(time.Now().Unix())*1000 - 50000),
		DeviceStatus: 1,
		Data:         []map[string]interface{}{{"CA": 10, "CSA": 1, "CB": 10, "CSB": 2}}})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	//go p.Wait()
	fmt.Println("Listen2")
	for {
		select {
		case <-signals:
			fmt.Println("Notify Processor")
			//p.signal<-sig
			return
		}
	}

}
