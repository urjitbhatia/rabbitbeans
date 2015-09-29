package main

import (
	"github.com/urjitbhatia/rabbitbeans/beans"
	"github.com/urjitbhatia/rabbitbeans/rabbit"
	"github.com/streadway/amqp"
	"sync"
)

func main() {

	jobs := make(chan amqp.Delivery) // channel joining rabbit to beanstalkd
	var waitGroup sync.WaitGroup

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		config := rabbit.Config{}
		config.QName = "scheduler"
		rabbit.InitAndListenQueue(config, jobs)
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		config := beans.Config{
			"127.0.0.1",
			"11300",
		}
		beans.InitAndPublishJobs(config, jobs)
	}()

	waitGroup.Wait()
}
