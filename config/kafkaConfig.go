package config

import (
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

func InitKafkaConsumer() (sarama.Consumer, error) {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Println("error al crear el consumidor de kafka:", err)
		return nil, err
	}

	return consumer, nil
}
