package main

import (
	"github.com/codegangsta/cli"
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans/beans"
	"github.com/urjitbhatia/rabbitbeans/rabbit"
	"log"
	"sync"
	"time"
)

func RunApp(c *cli.Context) {
	println("Running!")
	if !c.Bool("testMode") {
		println("service mode")
		service()
	} else {
		println("test mode")
		if c.Int("rabbitToBeans") > 0 {
			rabbitToBeans(c.Int("rabbitToBeans"), c.Int("wait"), c.Int("concurrency"))
		}

		if c.Int("beanToRabbit") > 0 {
			beanToRabbit(c.Int("beanToRabbit"), c.Int("wait"), c.Int("concurrency"))
		}
	}
}

func beanToRabbit(n, wait, concurrency int) {

	log.Println("Testing BeanToRabbit mode. Total rabbit writers:", concurrency)
	jobs := make(chan beans.Bean)

	var waitGroup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		log.Println("Adding producer:", i)
		waitGroup.Add(1)
		ProduceRabbits(waitGroup, jobs)
	}

	testBeans := &beans.TestBeanHandler{n, wait}
	start := time.Now()
	testBeans.Consume(jobs)
	waitGroup.Done() // force close ProduceRabbits
	waitGroup.Wait()

	elapsed := time.Since(start)
	log.Printf("Done consuming! %s. Total messages: %d", elapsed, n)
}

func rabbitToBeans(n, wait, concurrency int) {

	log.Println("Testing RabbitToBean mode. Total bean writers:", concurrency)
	jobs := make(chan amqp.Delivery)
	var waitGroup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		println("Adding consumer")
		waitGroup.Add(1)
		ProduceBeans(waitGroup, jobs)
	}

	testRabbits := &rabbit.TestRabbitHandler{n, wait}
	start := time.Now()
	testRabbits.Consume(jobs)
	waitGroup.Done() // force close ConsumeBeans
	waitGroup.Wait()

	elapsed := time.Since(start)
	log.Printf("Done consuming! %s. Total messages: %d", elapsed, n)
}
