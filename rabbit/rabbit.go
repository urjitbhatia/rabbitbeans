package rabbit

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans"
	"log"
	"time"
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
	Quiet      bool
}

// Connection captures the config used to connect to rabbitMq and
// the internal amqp connection as well. This connection object can then be used
// to multiplex multiple Producers and Consumers
type Connection struct {
	config           Config
	rabbitConnection *amqp.Connection
}

type RabbitHandler interface {
	WriteToRabbit(queueName string, jobs <-chan interface{})
	ReadFromRabbit(queueName string, jobs chan<- interface{})
}

type RabbitAcknowledger struct {
	d amqp.Delivery
}

func (r RabbitAcknowledger) Ack(id uint64) error {
	return r.d.Ack(false)
}

func (r RabbitAcknowledger) Nack(id uint64) error {
	return r.d.Nack(false, true)
}

// WriteToRabbit connects to the rabbitMQ queue defined in the config
// (if it does not exit, it will error). Then it pushes to messages on that
// queue whenever it gets a new one on the jobs channel.
func (conn *Connection) WriteToRabbit(queueName string, jobs <-chan interface{}) {

	ch, err := conn.rabbitConnection.Channel()
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
	for j := range jobs {
		job, ok := j.(rabbitbeans.Job)
		if !ok {
			rabbitbeans.LogOnError(errors.New("Unreadable job on the channel"), "Skipping item")
			continue
		}
		if !conn.config.Quiet {
			log.Printf("Sending rabbit to queue: %s", string(job.Body))
		}
		err = ch.Publish(
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: job.ContentType,
				Body:        job.Body,
			})
		if err != nil {
			job.Nack(job.Id)
		} else {
			// only ack the source delivery when the destination acks the publishing
			if confirmed := <-confirms; confirmed.Ack {
				job.Ack(job.Id)
			} else {
				job.Nack(job.Id)
			}
			rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to find queue named: %s", queueName))
		}
	}
}

// ReadFromRabbit connects to the rabbitMQ queue defined in the config
// (if it does not exit, it will error). Then it listens to messages on that
// queue and redirects then to the jobs channnel
func (conn *Connection) ReadFromRabbit(queueName string, jobs chan<- interface{}) {

	ch, err := conn.rabbitConnection.Channel()
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
		if !conn.config.Quiet {
			log.Printf("Received a rabbit from queue: %s", d.Body)
		}
		job := &rabbitbeans.Job{
			0,
			d.Body,
			//			d.Ack,
			//			d.Nack,
			RabbitAcknowledger{d},
			d.ContentType,
			uint32(d.Priority),
			0,
			time.Minute * 50, // TTR 50 minutes
			"",
		}
		jobs <- *job
	}
}

// Dial connects to an amqp URL where it expects a rabbitMQ instance to be running.
// Returns a multiplexable connection that can then be used to produce/consume on different queues
func Dial(config Config) RabbitHandler {

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
