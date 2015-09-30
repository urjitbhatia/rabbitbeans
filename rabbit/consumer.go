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
	AmqpUrl    string      // amqp host url
	AmqpConfig amqp.Config // amqp config
}

type Connection struct {
	config         Config
	amqpConnection *amqp.Connection
}

// InitAndListenQueue connects to the rabbitMQ queue defined in the config
// (if it does not exit, it will error). Then it listens to messages on that
// queue and redirects then to the jobs channnel
func (conn *Connection) Produce(queueName string, jobs <-chan string) {

	ch, err := conn.amqpConnection.Channel()
	rabbitbeans.FailOnError(err, "Failed to open a channel")
	defer ch.Close() // Clean up by closing channel when function exits

	q, err := ch.QueueInspect( // Make sure queue exists - don't create one otherwise and err.
		queueName, // queue name
	)
	rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to find queue named: %s", queueName))
	log.Printf("Connected to queue: %s", q.Name)

	log.Printf(" [*] Sending rabbits. To exit press CTRL+C")
	for d := range jobs {
		log.Printf("Sending message: %s", d)
		err = ch.Publish(
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(d),
			})
		rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to find queue named: %s", queueName))
	}
}

// InitAndListenQueue connects to the rabbitMQ queue defined in the config
// (if it does not exit, it will error). Then it listens to messages on that
// queue and redirects then to the jobs channnel
func (conn *Connection) Consume(queueName string, jobs chan<- amqp.Delivery) {

	ch, err := conn.amqpConnection.Channel()
	rabbitbeans.FailOnError(err, "Failed to open a channel")
	defer ch.Close() // Clean up by closing channel when function exits

	q, err := ch.QueueInspect( // Make sure queue exists - don't create one otherwise and err.
		queueName, // queue name
	)
	rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to find queue named: %s", queueName))
	log.Printf("Connected to queue: %s", queueName)

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

	log.Printf(" [*] Waiting for rabbits. To exit press CTRL+C")
	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)
		jobs <- d
	}
}

func Dial(config Config) *Connection {

	if config.AmqpUrl == "" {
		config.AmqpUrl = LocalhostAmqpUrl
	}

	conn, err := amqp.DialConfig(config.AmqpUrl, config.AmqpConfig)
	rabbitbeans.FailOnError(err, "Failed to connect to RabbitMQ")
	return &Connection{
		config,
		conn,
	}
}
