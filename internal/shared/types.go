package shared

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type Message struct {
	Metadata *kafka.TopicPartition
	Event *repo.Event
}

func NewMessage(metadata *kafka.TopicPartition, data []byte) *Message {
	
}