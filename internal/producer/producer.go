package producer

import (
	"fmt"
	// "partone/internal/shared"

	"github.com/elbrusnabiyev/kafkafromkcode-partone/internal/shared"
	"github.com/sirupsen/logrus"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer() *KafkaProducer {

	cfg := shared.NewKafkaConfig()

	/*if topic == "" {
		topic = cfg.Topic
	} */
	topic := cfg.DefaultTopic

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
	if err != nil {
		panic(err)
	}

	// defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					// fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					logrus.WithFields(logrus.Fields{
						"PRTN":   ev.TopicPartition.Partition,
						"OFFSET": ev.TopicPartition.Offset,
					}).Info("Delivered message")
				}
			}
		}
	}()

	return &KafkaProducer{
		producer: p,
		topic:    topic,
	}
}

func (p *KafkaProducer) Produce(msg []byte) {
	cfg := shared.NewKafkaConfig()
	topic := cfg.DefaultTopic
	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          msg,
	}, nil)

	/* err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          msg,
	}, nil)
	if err != nil {
		fmt.Printf("error producing msg := %v\n", err)
	} */

}
