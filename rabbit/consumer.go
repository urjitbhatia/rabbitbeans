package rabbit

import (
	"fmt"
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans"
	"log"
)

const (
	LocalhostAmqpUrl = "amqp://guest:guest@localhost:5672/"
)

// Config captures the fields for defining connection parameters
// QName holds the name of the queue to connect to
// AmqpUrl holds the amql url string and
// AmqpConfig holds the advanced AmqpConfig like Heartbeat etc
type Config struct {
	QName      string      // which queue should this connect to
	AmqpUrl    string      // amqp host url
	AmqpConfig amqp.Config // amqp config
}

// InitAndListenQueue connects to the rabbitMQ queue defined in the config
// (if it does not exit, it will error). Then it listens to messages on that
// queue and redirects then to the jobs channnel
func InitAndListenQueue(config Config, jobs chan<- amqp.Delivery) {

	if config.AmqpUrl == "" {
		config.AmqpUrl = LocalhostAmqpUrl
	}

	conn, err := amqp.DialConfig(config.AmqpUrl, config.AmqpConfig)
	rabbitbeans.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close() // Clean up by closing connection when function exits

	ch, err := conn.Channel()
	rabbitbeans.FailOnError(err, "Failed to open a channel")
	defer ch.Close() // Clean up by closing channel when function exits

	q, err := ch.QueueInspect( // Make sure queue exists - don't create one otherwise and err.
		config.QName, // queue name
	)

	rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to find queue named: %s", config.QName))

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	rabbitbeans.FailOnError(err, "Failed to register a consumer")

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
