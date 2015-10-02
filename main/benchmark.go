package main

import (
	"github.com/codegangsta/cli"
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans/beans"
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
		uri := "amqp://guest:guest@" + c.String("server") + ":5672"
		if c.Int("rabbitToBeans") > 0 {
			rabbitToBeans(uri, c.Int("concurrency"), c.Int("consumer"))
		}

		if c.Int("beanToRabbit") > 0 {
			beanToRabbit(c.Int("beanToRabbit"), c.Int("wait"), c.Int("concurrency"))
		}
	}
}

func beanToRabbit(n int, wait int, concurrency int) {

	println("Adding producers")
	jobs := make(chan beans.Bean)
	var waitGroup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		println("Adding producer")
		waitGroup.Add(1)
		ProduceRabbits(waitGroup, jobs)
	}
	start := time.Now()
	testBeans := &beans.TestBeanHandler{n, wait}
	testBeans.Consume(jobs)
	waitGroup.Done() // force close ProduceRabbits
	waitGroup.Wait()
	log.Printf("Done consuming! %s", time.Since(start))
}

func rabbitToBeans(uri string, concurrency int, toConsume int) {
	println("Adding consumersss")
	jobs := make(chan amqp.Delivery)
	var waitGroup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		println("Adding consumer")
		waitGroup.Add(1)
		ConsumeRabbits(waitGroup, jobs)
	}
	start := time.Now()

	if toConsume > 0 {
		for i := 0; i < toConsume; i++ {
			<-jobs
			if i == 1 {
				start = time.Now()
			}
			log.Println("Consumed: ", i)
		}
	} else {

		for {
			<-jobs
		}
	}
	waitGroup.Wait()
	log.Printf("Done consuming! %s", time.Since(start))
}
