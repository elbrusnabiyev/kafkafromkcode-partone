package main

import (
	"fmt"
	"partone/internal/consumer"
	"partone/internal/producer"
	"time"
)

type Server struct {
	producer *producer.KafkaProducer
	consumer *consumer.KafkaConsumer
	msgCH    chan string
}

func NewServer() *Server {
	msgCH := make(chan string, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)
	if err != nil {
		panic(err)
	}
	return &Server{
		producer: producer.NewKafkaProducer(""),
		consumer: c,
		msgCH:    msgCH,
	}
}

func (s *Server) produceMsg() {

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	id := 0
	for t := range ticker.C {
		msg := fmt.Sprintf("hello from Kafka, msgID = %d, ts = %s", id, t.Format("15:20:20"))
		id++
		s.producer.Produce(msg)

	}
}

func (s *Server) handleMsg(msg string) {
	// db operation or other bussines-logic
	fmt.Printf("received msg = %s\n", msg)
}

func main() {
	s := NewServer()

	go s.produceMsg()
	for msg := range s.msgCH {
		go s.handleMsg(msg)
	}

}
