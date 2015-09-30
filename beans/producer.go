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
	protocol = "tcp"
	defaultHost = "127.0.0.1"
	defaultPort = "11300"
)

// Config caputures the fields for defining connection parameters
// Host points to beanstalkd host
// Port points to the port beanstalkd host is listening on
type Config struct {
	Host string
	Port string
}

func InitAndPublishJobs(config Config, jobs <-chan amqp.Delivery) {

    if config.Host == "" {
        config.Host = defaultHost
    }
    if config.Port == "" {
        config.Port = defaultPort
    }

	var connString = fmt.Sprintf("%s:%s", config.Host, config.Port)
	conn, err := beanstalk.Dial(protocol, connString)
	rabbitbeans.FailOnError(err, fmt.Sprintf("Failed to connect to Beanstalkd at: %s", connString))

	forever := make(chan bool)

	go func() {
		for d := range jobs {
			log.Printf("Received a message: %s", d.Body)
			id, err := conn.Put(
				d.Body, //body
				0,      //pri uint32
				0,      //delay
				10*60*time.Second, // TTR time to run -- is an integer number of seconds to allow a worker to run this job
			)
			rabbitbeans.LogOnError(err, fmt.Sprintf("Failed to put job on beanstalkd %s", d.Body))
			fmt.Println("Created job", id)
		}
	}()

	log.Printf(" [*] Waiting for beans. To exit press CTRL+C")
	<-forever
}
