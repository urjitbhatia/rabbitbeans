package main

import (
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans/beans"
	"github.com/urjitbhatia/rabbitbeans/rabbit"
	"sync"
)

func main() {

	// The channel that takes in rabbits (rabbit jobs) and delivers them to beans (beanstalkd)
	jobs := make(chan amqp.Delivery)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	consumeRabbits(waitGroup, jobs)

	waitGroup.Add(1)
	produceBeans(waitGroup, jobs)

	waitGroup.Wait()
}

func consumeRabbits(waitGroup sync.WaitGroup, jobs chan amqp.Delivery) {
	config := rabbit.Config{}
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	go func() {
		defer waitGroup.Done()
		rabbitConn.Consume(queueName, jobs)
	}()
}

func produceBeans(waitGroup sync.WaitGroup, jobs <-chan amqp.Delivery) {
	go func() {
		defer waitGroup.Done()
		config := beans.Config{
			"127.0.0.1",
			"11300",
		}
		beans.InitAndPublishJobs(config, jobs)
	}()
}

func produceRabbits(waitGroup sync.WaitGroup, jobs <-chan string) {
	config := rabbit.Config{}
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	toSend := []string{"first produce", "second produce"}
	sendChan := make(chan string)
	go func() {
		defer waitGroup.Done()
		rabbitConn.Produce(queueName, sendChan)
	}()
	for _, ts := range toSend {
		sendChan <- ts
	}
}
