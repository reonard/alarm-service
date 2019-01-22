package main

import (
	"alarm-service/consumer"
	"alarm-service/db"
	. "alarm-service/lib"
	"alarm-service/processor"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
)

func main() {

	InitCfg()
	fmt.Println(AppCfg.AlarmTopics)
	kafkaConsumer := consumer.NewKafkaConsumer(
		[]string{"120.77.245.156:9092"},
		"DT03",
		[]string{"alarm"})

	defer kafkaConsumer.Close()

	s := db.InitMongoDB(AppCfg.MongodbURL)
	defer s.Close()

	m := db.InitMySQLDB(AppCfg.MySqlURL)
	defer m.Close()

	db.InitCache()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	alarmProcessor := processor.NewProcessor(AppCfg.WorkerNum, AppCfg.BulkDataBuffer)
	alarmProcessor.Run()

	for {
		select {
		case msg, ok := <-kafkaConsumer.Messages():
			fmt.Println(msg)
			if ok {

				msgData := processor.MonitorData{}

				err := json.Unmarshal(msg.Value, &msgData)
				if err != nil {
					fmt.Println("Invalid Data")
					continue
				}
				alarmProcessor.AddData(&msgData)

				fmt.Fprintf(os.Stdout, "接收Kafka信息：主题-%s/分区-%d/偏移-%d\t消息-Key:%s\tValue:%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				kafkaConsumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			fmt.Println("Get Signal, Wait For Processor")
			alarmProcessor.Wait()
			fmt.Println("Processor Graceful Down, Bye")

			return
		}
	}
}
