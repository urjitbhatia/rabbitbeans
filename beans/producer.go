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

type Bean struct {
	Id   uint64
	Body []byte
}

type Connection struct {
	config          Config
	beansConnection *beanstalk.Conn
}

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

func (conn *Connection) Consume(jobs chan<- Bean) {

	log.Printf(" [*] Consuming beans. To exit press CTRL+C")

	ticker := time.NewTicker(time.Duration(conn.config.TickInterval) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
			    log.Printf("Polling again...")
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
				}
				conn.beansConnection.Delete(id)
			}
		}
	}()
}

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
