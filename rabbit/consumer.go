package rabbit

import (
	//	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans"
	"github.com/urjitbhatia/rabbitbeans/beans"
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

// Connection captures the config used to connect to rabbitMq and
// the internal amqp connection as well. This connection object can then be used
// to multiplex multiple Producers and Consumers
type Connection struct {
	config           Config
	rabbitConnection AmqpConnection
}

type AmqpConnection interface {
	Channel() (interface{}, error)
}

// Produce connects to the rabbitMQ queue defined in the config
// (if it does not exit, it will error). Then it pushes to messages on that
// queue whenever it gets a new one on the jobs channel.
func (conn Connection) Produce(queueName string, jobs <-chan beans.Bean) {

	c, err := conn.rabbitConnection.Channel()
	ch, ok := c.(*amqp.Channel)
	if !ok {
		log.Fatal("Fuck that shit")
	}
	rabbitbeans.FailOnError(err, "Failed to open a channel")
	defer ch.Close() // Clean up by closing channel when function exits

	q, err := ch.QueueInspect( // Make sure queue exists - don't create one otherwise and err.
		queueName, // queue name
	)
	rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to find queue named: %s", queueName))
	log.Printf("Connected to queue: %s", q.Name)

	if err := ch.Confirm(
		false, // noWait = false - means, please do wait for confirms
	); err != nil {
		rabbitbeans.FailOnError(err, "Could not set channel confirm mode on")
	}

	// Buffer of 1 for our single outstanding publishing
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	log.Printf(" [*] Sending rabbits. To exit press CTRL+C")
	for job := range jobs {
		log.Printf("Sending rabbit to queue: %s", job.Body)
		err = ch.Publish(
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(job.Body),
			})
		if err != nil {
			job.Nack()
		} else {
			// only ack the source delivery when the destination acks the publishing
			if confirmed := <-confirms; confirmed.Ack {
				job.Ack()
			} else {
				job.Nack()
			}
			rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to find queue named: %s", queueName))
		}
	}
}

// Consume connects to the rabbitMQ queue defined in the config
// (if it does not exit, it will error). Then it listens to messages on that
// queue and redirects then to the jobs channnel
func (conn Connection) Consume(queueName string, jobs chan<- amqp.Delivery) {

	c, err := conn.rabbitConnection.Channel()
	ch, ok := c.(*amqp.Channel)
	if !ok {
		log.Fatal("Fuck that shit")
	}
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
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	rabbitbeans.FailOnError(err, "Failed to register a consumer")

	log.Printf(" [*] Waiting for rabbits. To exit press CTRL+C")
	for d := range msgs {
		log.Printf("Received a rabbit from queue: %s", d.Body)
		jobs <- d
	}
}

// Dial connects to an amqp URL where it expects a rabbitMQ instance to be running.
// Returns a multiplexable connection that can then be used to produce/consume on different queues
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
