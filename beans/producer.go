package beans

import (
	"fmt"
	"github.com/kr/beanstalk"
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans"
	"log"
	"time"
)

const (
	protocol            = "tcp"
	defaultHost         = "127.0.0.1"
	defaultPort         = "11300"
	defaultTickInterval = 2 //seconds
)

// Config caputures the fields for defining connection parameters
// Host points to beanstalkd host
// Port points to the port beanstalkd host is listening on
type Config struct {
	Host         string
	Port         string
	TickInterval int
}

// Bean captures a "job" from beanstalkd
type Bean struct {
	Id   uint64
	Body []byte
	Ack  func()
	Nack func()
}

// Connection captures the config used to connect to beanstalkd and
// the internal beanstalkd connection as well. This connection object can then be used
// to multiplex multiple produce/consume actions
type Connection struct {
	config          Config
	beansConnection *beanstalk.Conn
}

// Publish puts jobs onto beanstalkd. The jobs channel expects messages of type amqp.Delivery
func (conn *Connection) Publish(jobs <-chan amqp.Delivery) {

	log.Printf(" [*] Publishing beans. To exit press CTRL+C")
	for d := range jobs {
		log.Printf("Received a message: %s", d.Body)
		id, err := conn.beansConnection.Put(
			d.Body, //body
			0,      //pri uint32
			0,      //delay
			10*60*time.Second, // TTR time to run -- is an integer number of seconds to allow a worker to run this job
		)
		rabbitbeans.LogOnError(err, fmt.Sprintf("Failed to put job on beanstalkd %s", d.Body))
		fmt.Println("Created job", id)
	}
}

// Consumes jobs off of beanstalkd. The jobs channel posts messages of type beans.Bean
func (conn *Connection) Consume(jobs chan<- Bean) {

	log.Printf(" [*] Consuming beans. To exit press CTRL+C")

	ticker := time.NewTicker(time.Duration(conn.config.TickInterval) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Printf("Polling beanstalkd for beans")
				id, body, err := conn.beansConnection.Reserve(5 * time.Second)
				if cerr, ok := err.(beanstalk.ConnError); !ok {
					rabbitbeans.FailOnError(err, "expected connError")
				} else if cerr.Err != beanstalk.ErrTimeout {
					rabbitbeans.FailOnError(err, "expected timeout on reserve")
				}
				log.Printf("Reserved job %v %s", id, body)
				jobs <- Bean{
					id,
					body,
					func() {
					    log.Println("Job acked...")
						conn.beansConnection.Delete(id)
					},
					func() {
					    log.Println("Job nacked...")
						conn.beansConnection.Release(id, 0, time.Second*1)
					},
				}
			}
		}
	}()
}

// Dial connects to a beanstalkd instance.
// Returns a multiplexable connection that can then be used to put/reserve jobs.
func Dial(config Config) *Connection {

	if config.Host == "" {
		config.Host = defaultHost
	}
	if config.Port == "" {
		config.Port = defaultPort
	}
	if config.TickInterval == 0 {
		config.TickInterval = defaultTickInterval
	}

	var connString = fmt.Sprintf("%s:%s", config.Host, config.Port)
	conn, err := beanstalk.Dial(protocol, connString)
	rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to connect to Beanstalkd at: %s", connString))
	return &Connection{
		config,
		conn,
	}
}
