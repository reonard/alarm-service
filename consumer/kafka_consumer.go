package consumer

import (
	"github.com/bsm/sarama-cluster"
	"log"
)

func NewKafkaConsumer(brokerList []string, groupID string, topics []string) *cluster.Consumer {

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	consumer, err := cluster.NewConsumer(brokerList, groupID, topics, config)
	if err != nil {
		panic(err)
	}

	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	return consumer

}
