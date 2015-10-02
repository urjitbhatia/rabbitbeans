package beans

import (
	"fmt"
	"github.com/kr/beanstalk"
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
	Host         string // the beanstalkd host
	Port         string // the beanstalkd host port
	TickInterval int    // the time to wait between successive polls of beanstalkd
	Quiet        bool   // quiet logs
	BatchSize    int    // the number of jobs to pull from beanstalkd per poll (if less jobs are ready, it defers to next poll)
}

// Connection captures the config used to connect to beanstalkd and
// the internal beanstalkd connection as well. This connection object can then be used
// to multiplex multiple produce/consume actions
type Connection struct {
	config          Config
	beansConnection *beanstalk.Conn
}

type BeanHandler interface {
	WriteToBeanstalkd(<-chan rabbitbeans.Job)
	ReadFromBeanstalkd(chan<- rabbitbeans.Job)
}

// Publish puts jobs onto beanstalkd. The jobs channel expects messages of type amqp.Delivery
func (conn *Connection) WriteToBeanstalkd(jobs <-chan rabbitbeans.Job) {

	log.Printf(" [*] Publishing beans. To exit press CTRL+C")
	for job := range jobs {
		if !conn.config.Quiet {
			log.Printf("Received a bean to create: %s", job.Body)
		}
		id, err := conn.beansConnection.Put(
			job.Body,     //body
			job.Priority, //pri uint32
			job.Delay,    //delay
			job.TTR,      // TTR time to run -- is an integer number of seconds to allow a worker to run this job
		)
		rabbitbeans.LogOnError(err, fmt.Sprintf("Failed to put job on beanstalkd %s", job.Body))
		if err != nil {
			job.Nack(id)
		} else {
			job.Ack(id)
		}
		if !conn.config.Quiet {
			fmt.Println("Created job", id)
		}
	}
}

// Consumes jobs off of beanstalkd. The jobs channel posts messages of type beans.Bean
func (conn *Connection) ReadFromBeanstalkd(jobs chan<- rabbitbeans.Job) {

	log.Printf(" [*] Consuming beans. To exit press CTRL+C")

	ticker := time.NewTicker(time.Duration(conn.config.TickInterval) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Printf("Polling beanstalkd for beans")
				var i = 0
				for {
					log.Printf("bean read")
					id, body, err := conn.beansConnection.Reserve(5 * time.Second)
					if cerr, ok := err.(beanstalk.ConnError); !ok {
						rabbitbeans.FailOnError(err, "expected connError")
					} else if cerr.Err != beanstalk.ErrTimeout {
						rabbitbeans.LogOnError(err, fmt.Sprintf("expected timeout on reserve %d", id))
						// Means the job deadline is real soon!! Reserve job anyways
					} else {
						break
					}
					if !conn.config.Quiet {
						log.Printf("Reserved job %v %s", id, body)
					}
					jobs <- rabbitbeans.Job{
						id,
						body,
						conn,
						"application/json",
						0,
						0,
						0,
						"",
					}
					log.Printf("bean dispatched")
					i++
					if i == conn.config.BatchSize {
						break
					}
				}
				log.Printf("Processed %d jobs this tick", i)
			}
		}
	}()
}

// Implements the Acknowledger interface Ack
func (c *Connection) Ack(id uint64) error {
	return c.beansConnection.Delete(id)
}

// Implements the Acknowledger interface Nack
func (c *Connection) Nack(id uint64) error {
	return c.beansConnection.Release(id, 0, time.Second*2)
}

// Dial connects to a beanstalkd instance.
// Returns a multiplexable connection that can then be used to put/reserve jobs.
func Dial(config Config) BeanHandler {

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
