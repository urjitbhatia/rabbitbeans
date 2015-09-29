package beans

import (
	"fmt"
	"github.com/kr/beanstalk"
	"github.com/streadway/amqp"
	"log"
	"time"
)

const (
	protocol = "tcp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

type Config struct {
	Host string
	Port string
}

func InitAndPublishJobs(config Config, jobs <-chan amqp.Delivery) {

	var connString = fmt.Sprintf("%s:%s", config.Host, config.Port)
	conn, err := beanstalk.Dial(protocol, connString)
	failOnError(err, fmt.Sprintf("Failed to connect to Beanstalkd at: %s", connString))

	forever := make(chan bool)

	go func() {
		for d := range jobs {
			log.Printf("Received a message: %s", d.Body)
			id, err := conn.Put(
				d.Body, //body
				0, //pri uint32
				0, //delay
				10*60*time.Second, // TTR time to run -- is an integer number of seconds to allow a worker to run this job
			)
			failOnError(err, fmt.Sprintf("Failed to put job on beanstalkd %s", d.Body))
			fmt.Println("Created job", id)
		}
	}()

	log.Printf(" [*] Waiting for beans. To exit press CTRL+C")
	<-forever
}
