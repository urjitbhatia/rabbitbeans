package main

import (
	"github.com/codegangsta/cli"
	"github.com/urjitbhatia/rabbitbeans"
	"github.com/urjitbhatia/rabbitbeans/beans"
	"github.com/urjitbhatia/rabbitbeans/rabbit"
	"log"
	"sync"
	"time"
)

func RunApp(c *cli.Context) {
	println("Running!")
	if !c.Bool("testMode") {
		println("Running in Service mode")
		service()
	} else {
		println("Running in Test mode")
		if c.Int("BeanWrite") > 0 {
			beanWrite(c.Int("BeanWrite"), c.Int("wait"), c.Int("concurrency"))
		}

		if c.Int("BeanRead") > 0 {
			// Write fake beans first, then read
			beanWrite(c.Int("BeanRead"), 0, 1)
			beanRead(c.Int("BeanRead"), c.Int("wait"), c.Int("concurrency"))
		}

		if c.Int("RabbitWrite") > 0 {
			rabbitWrite(c.Int("RabbitWrite"), c.Int("wait"), c.Int("concurrency"))
		}

		if c.Int("RabbitRead") > 0 {
			rabbitRead(c.Int("RabbitRead"), c.Int("wait"), c.Int("concurrency"))
		}
	}
}

/*
 * Benchmark that reads off in-memory Beanstalkd stub and writes to real Rabbit
 */
func rabbitWrite(n, wait, concurrency int) {

	log.Println("Testing rabbitWrites. Total rabbit writers:", concurrency)
	messages := make(chan rabbitbeans.Job)

	var waitGroup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		log.Println("Adding rabbit writer:", i)
		waitGroup.Add(1)
		WriteToRabbit(waitGroup, messages)
	}

	// Fake Beans
	testBeans := &beans.TestBeanHandler{n, wait}
	start := time.Now()
	testBeans.ReadFromBeanstalkd(messages)
	waitGroup.Done() // force close ProduceRabbits
	waitGroup.Wait()

	elapsed := time.Since(start)
	log.Printf("Done writing to rabbit! %s. Total messages: %d", elapsed, n)
}

/*
 * Benchmark that reads off real Rabbit and writes to in-memory Beanstalkd stub
 */
func rabbitRead(n, wait, concurrency int) {

	log.Println("Testing rabbitReads. Total rabbit readers:", concurrency)
	messages := make(chan rabbitbeans.Job)

	var waitGroup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		log.Println("Adding rabbit reader:", i)
		waitGroup.Add(1)
		ReadFromRabbit(waitGroup, messages)
	}

	// Fake Beans
	testBeans := &beans.TestBeanHandler{n, wait}
	start := time.Now()
	testBeans.WriteToBeanstalkd(messages)
	waitGroup.Done() // force close ProduceRabbits
	waitGroup.Wait()

	elapsed := time.Since(start)
	log.Printf("Done reading from rabbit! %s. Total messages: %d", elapsed, n)
}

/*
 * Benchmark that reads off in-memory Rabbit stub and writes to real Beanstalkd
 */
func beanWrite(n, wait, concurrency int) {

	log.Println("Testing beanWrites. Total bean writers:", concurrency)
	jobs := make(chan rabbitbeans.Job)
	var waitGroup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		println("Adding beanstalkd writer")
		waitGroup.Add(1)
		WriteToBeanstalkd(waitGroup, jobs)
	}

	// Fake Rabbits
	testRabbits := &rabbit.TestRabbitHandler{n, wait}
	start := time.Now()
	testRabbits.ReadFromRabbit(jobs)
	waitGroup.Done() // force close ConsumeBeans
	waitGroup.Wait()

	elapsed := time.Since(start)
	log.Printf("Done writing to beanstalkd! %s. Total jobs: %d", elapsed, n)
}

/*
 * Benchmark that reads off real Beanstalkd and writes to in-memory Rabbit stub
 */
func beanRead(n, wait, concurrency int) {

	log.Println("Testing beanReads. Total bean readers:", concurrency)
	jobs := make(chan rabbitbeans.Job)

	var waitGroup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		println("Adding beanstalkd reader")
		waitGroup.Add(1)
		ReadFromBeanstalkd(waitGroup, jobs)
	}

	// Fake Rabbits
	testRabbits := &rabbit.TestRabbitHandler{n, wait}
	start := time.Now()
	testRabbits.WriteToRabbit(jobs)
	waitGroup.Done() // force close ConsumeBeans
	waitGroup.Wait()

	elapsed := time.Since(start)
	log.Printf("Done reading from beanstalkd! %s. Total jobs: %d", elapsed, n)
}
