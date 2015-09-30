package main

import (
	"encoding/json"
	"errors"
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans"
	"github.com/urjitbhatia/rabbitbeans/beans"
	"github.com/urjitbhatia/rabbitbeans/rabbit"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

type Config struct {
	RabbitToBean bool // select mode. RabbitToBean consumes from rabbitMq and publishes to Beanstalkd if true, reverse otherwise
}

const (
	defaultRabbitToBean = false
)

func main() {
	config := configure()
	// The channel that takes in rabbits (rabbit jobs) and delivers them to beans (beanstalkd)
	var waitGroup sync.WaitGroup
	if config.RabbitToBean {
		jobs := make(chan amqp.Delivery)

		waitGroup.Add(1)
		consumeRabbits(waitGroup, jobs)

		waitGroup.Add(1)
		produceBeans(waitGroup, jobs)
	} else {
		jobs := make(chan beans.Bean)

		waitGroup.Add(1)
		consumeBeans(waitGroup, jobs)

		waitGroup.Add(1)
		produceRabbits(waitGroup, jobs)
	}

	waitGroup.Wait()
}

func consumeRabbits(waitGroup sync.WaitGroup, jobs chan<- amqp.Delivery) {
	config := rabbit.Config{}
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	go func() {
		defer waitGroup.Done()
		rabbitConn.Consume(queueName, jobs)
	}()
}

func consumeBeans(waitGroup sync.WaitGroup, jobs chan<- beans.Bean) {
	go func() {
		defer waitGroup.Done()
		config := beans.Config{
			"127.0.0.1",
			"11300",
			2,
		}
		beansConn := beans.Dial(config)
		beansConn.Consume(jobs)
	}()
}

func produceBeans(waitGroup sync.WaitGroup, jobs <-chan amqp.Delivery) {
	go func() {
		defer waitGroup.Done()
		config := beans.Config{
			"127.0.0.1",
			"11300",
			0,
		}
		beansConn := beans.Dial(config)
		beansConn.Publish(jobs)
	}()
}

func produceRabbits(waitGroup sync.WaitGroup, jobs <-chan beans.Bean) {
	config := rabbit.Config{}
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	//	toSend := []string{"first produce", "second produce"}
	//	sendChan := make(chan string)
	go func() {
		defer waitGroup.Done()
		rabbitConn.Produce(queueName, jobs)
	}()
	//	for _, ts := range toSend {
	//		sendChan <- ts
	//	}
}

func configure() Config {
	var config Config

	if len(os.Args) < 2 {
		msg := "Missing config file"
		rabbitbeans.LogOnError(errors.New(msg), "Starting in rabbitToBean mode by default")
		config.RabbitToBean = defaultRabbitToBean
	} else {
		data, readErr := ioutil.ReadFile(os.Args[1])
		rabbitbeans.LogOnError(readErr, "Starting in rabbitToBean mode by default")

		jsonErr := json.Unmarshal(data, &config)
		rabbitbeans.LogOnError(jsonErr, "Starting in rabbitToBean mode by default")
	}
	log.Printf("Starting with config: %v", config)
	return config
}
