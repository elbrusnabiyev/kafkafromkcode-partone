package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"

	"github.com/elbrusnabiyev/kafkafromkcode-partone/internal/consumer"
	"github.com/elbrusnabiyev/kafkafromkcode-partone/internal/producer"
	"github.com/elbrusnabiyev/kafkafromkcode-partone/internal/repo"
	"github.com/elbrusnabiyev/kafkafromkcode-partone/internal/shared"
	"github.com/jmoiron/sqlx"

	"time"
)

type Server struct {
	addr      string
	producer  *producer.KafkaProducer
	consumer  *consumer.KafkaConsumer
	msgCH     chan *shared.Message
	eventRepo *repo.EventRepo
}

func NewServer(addr string, eventRepo *repo.EventRepo) *Server {
	/* msgCH := make(chan *shared.Message, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)
	if err != nil {
		panic(err)
	} */

	return &Server{
		// producer:  producer.NewKafkaProducer(""),
		// consumer:  c,
		msgCH:     make(chan *shared.Message, 512),
		addr:      addr,
		eventRepo: eventRepo,
	}
}

func (s *Server) addProducer() *Server {
	shouldProduce := os.Getenv("SHOULD_PRODUCE") == "true"
	if !shouldProduce {
		shouldProduce = *flag.Bool("SHOULD_PRODUCE", false, "Enable message production")
		flag.Parse()
	}
	fmt.Printf("SHOULD_PRODUCE = %t\n", shouldProduce)

	if shouldProduce {
		s.producer = producer.NewKafkaProducer()
		go s.produceMsg()
	}
	return s
}

func (s *Server) addConsumer() *Server {
	c := consumer.NewKafkaConsumer(s.msgCH)
	s.consumer = c
	return s
}

func (s *Server) produceMsg() {

	ticker := time.NewTicker(2 * time.Second)
	// defer ticker.Stop()
	// id := 0
	for range ticker.C {
		// msg := fmt.Sprintf("hello from Kafka, msgID = %d, ts = %s", id, t.Format("15:20:20"))
		// id++
		event := repo.NewEvent()
		b, err := json.Marshal(event)
		if err != nil {
			// panic("unaable to marshal json")
			fmt.Printf("error marshaling event = %v\n", err)
			continue
		}
		s.producer.Produce(b)
	}
}

func (s *Server) handleMsg(msg *shared.Message) {
	// db operation or other bussines-logic

	<-s.consumer.ReadyCH
	r := time.Duration(rand.IntN(5))
	time.Sleep(r * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	/* rand := rand.IntN(5)
	time.Sleep(time.Duration(rand + 1))
	_, err := s.saveToDB(ctx, msg)
	if err != nil {
		fmt.Println("db error = %v\n", err)
	} */
	// fmt.Printf("received msg = %s\n", msg)
	defer cancel()
	_, err := s.saveToDB(ctx, msg)
	if err != nil {
		return
	}
}

// 1. do we need to get -> NO
// 2. do we lock db, higher isolation level -> NO
// 3. ctx + tx -> YES

func (s *Server) saveToDB(ctx context.Context, msg *shared.Message) (string, error) {
	return repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		/* fmt.Printf("starting DB operation for OFFSET = %d, EventID = %s\n", msg.Metadata.Offset, msg.Event.EventId)
		// TODO -> how handle insert error
		defer s.consumer.MarkAsComplete(msg.Metadata)
		// business logic
		event := s.eventRepo.Get(ctx, tx, msg.Event.EventId)
		if event != nil {
			eMsg := fmt.Sprintf("offset = %d, eventID %s already existing -> skipping\n", msg.Metadata.Offset, msg.Event.EventId)
			return "", errors.New(eMsg)
		}

		id, err := s.eventRepo.Insert(ctx, tx, msg.Event)
		if err != nil {
			return "", err
		}
		fmt.Printf("INSERT SUCCESS, EventID = %s, Offset = %d\n", id, msg.Metadata.Offset)
		return id, nil */
		id, err := s.eventRepo.Insert(ctx, tx, msg.Event)
		if err != nil {
			exists := repo.IsDDuplicateKeyErr(err)
			if exists {
				eMsg := fmt.Sprintf("already exists OFFSET = %d, PRTN = %d, EventID = %s\n", msg.Metadata.Offset, msg.Metadata.Partition, msg.Event.EventId)
				s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Success)
				return "", errors.New(eMsg)
			}
			s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Error)
			return "", err
		}
		s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Success)
		return id, nil
	})
}

func main() {
	db, err := repo.NewDBConn()
	if err != nil {
		panic(fmt.Sprintf("unable to conn to db, err = %v\n", err))
	}
	defer db.Close()

	er := repo.NewEventRepo(db)

	s := NewServer(":7576", er).addConsumer().addProducer()

	// go s.produceMsg()
	go func() {
		for msg := range s.msgCH {
			go s.handleMsg(msg)
		}
	}()

	s.consumer.RunConsumer()
}
