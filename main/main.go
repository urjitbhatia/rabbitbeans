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
