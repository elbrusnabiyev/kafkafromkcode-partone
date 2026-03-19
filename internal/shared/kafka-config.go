package shared

type KafkaPartitionStrategies string

const (
	CooperativeStickyStrategy KafkaPartitionStrategies = "cooperative-sticky"
	RoundRobin                KafkaPartitionStrategies = "roundrobin"
)

type KafkaConfig struct {
	DefaultTopic            string
	ConsumerGroup           string
	Host                    string
	PartitionAssignStrategy KafkaPartitionStrategies
	NumPartitions           int
}

func NewKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		PartitionAssignStrategy: CooperativeStickyStrategy,
		DefaultTopic:            "local_topic_sticky1",
		ConsumerGroup:           "local_cg1",
		Host:                    "localhost",
		NumPartitions:           4,
	}
}
