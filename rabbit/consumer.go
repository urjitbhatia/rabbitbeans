package rabbit

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

const (
	LocalhostAmqpUrl = "amqp://guest:guest@localhost:5672/"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

type Config struct {
	QName      string
	AmqpUrl    string
	AmqpConfig amqp.Config
}

func InitAndListenQueue(config Config, jobs chan<- amqp.Delivery) {

	if config.AmqpUrl == "" {
		config.AmqpUrl = LocalhostAmqpUrl
	}

	conn, err := amqp.DialConfig(config.AmqpUrl, config.AmqpConfig)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close() // Clean up by closing connection when function exits

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close() // Clean up by closing channel when function exits

	q, err := ch.QueueInspect( // Make sure queue exists - don't create one otherwise and err.
		config.QName, // queue name
	)

	failOnError(err, fmt.Sprintf("Failed to find queue named: %s", config.QName))

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			jobs <- d
		}
	}()

	log.Printf(" [*] Waiting for rabbits. To exit press CTRL+C")
	<-forever
}
