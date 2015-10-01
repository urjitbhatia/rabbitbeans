package main

import (
	"encoding/json"
	"errors"
	"github.com/codegangsta/cli"
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans"
	"github.com/urjitbhatia/rabbitbeans/beans"
	"github.com/urjitbhatia/rabbitbeans/rabbit"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

type Config struct {
	RabbitToBean bool // select mode. RabbitToBean consumes from rabbitMq and publishes to Beanstalkd if true, reverse otherwise
}

const (
	defaultRabbitToBean = false
)

func service() {
	config := configure()
	// The channel that takes in rabbits (rabbit jobs) and delivers them to beans (beanstalkd)
	var waitGroup sync.WaitGroup
	if config.RabbitToBean {
		jobs := make(chan amqp.Delivery)

		waitGroup.Add(1)
		ConsumeRabbits(waitGroup, jobs)

		waitGroup.Add(1)
		ProduceBeans(waitGroup, jobs)
	} else {
		jobs := make(chan beans.Bean)

		waitGroup.Add(1)
		ConsumeBeans(waitGroup, jobs)

		waitGroup.Add(1)
		ProduceRabbits(waitGroup, jobs)
	}

	waitGroup.Wait()
}

func ConsumeRabbits(waitGroup sync.WaitGroup, jobs chan<- amqp.Delivery) {
	config := rabbit.Config{}
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	go func() {
		defer waitGroup.Done()
		rabbitConn.Consume(queueName, jobs)
	}()
}

func ConsumeBeans(waitGroup sync.WaitGroup, jobs chan<- beans.Bean) {
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

func ProduceBeans(waitGroup sync.WaitGroup, jobs <-chan amqp.Delivery) {
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

func ProduceRabbits(waitGroup sync.WaitGroup, jobs <-chan beans.Bean) {
	config := rabbit.Config{}
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	go func() {
		defer waitGroup.Done()
		rabbitConn.Produce(queueName, jobs)
	}()
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

var totalTime int64 = 0
var totalCount int64 = 0

type MqMessage struct {
	TimeNow        time.Time
	SequenceNumber int
	Payload        string
}

func main() {
	app := cli.NewApp()
	app.Name = "tester"
	app.Usage = "Make the rabbit cry"
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "server, s", Value: "localhost", Usage: "Hostname for RabbitMQ server"},
		cli.IntFlag{Name: "producer, p", Value: 0, Usage: "Number of messages to produce, -1 to produce forever"},
		cli.IntFlag{Name: "wait, w", Value: 0, Usage: "Number of nanoseconds to wait between publish events"},
		cli.IntFlag{Name: "consumer, c", Value: -1, Usage: "Number of messages to consume. 0 consumes forever"},
		cli.IntFlag{Name: "bytes, b", Value: 0, Usage: "number of extra bytes to add to the RabbitMQ message payload. About 50K max"},
		cli.IntFlag{Name: "concurrency, n", Value: 1, Usage: "number of reader/writer Goroutines"},
		cli.BoolFlag{Name: "quiet, q", Usage: "Print only errors to stdout"},
		cli.BoolFlag{Name: "wait-for-ack, a", Usage: "Wait for an ack or nack after enqueueing a message"},
		cli.BoolFlag{Name: "testMode, t", Usage: "Run stress test mode. Runs as a service otherwise"},
	}
	app.Action = func(c *cli.Context) {
		runApp(c)
	}
	app.Run(os.Args)
}

func runApp(c *cli.Context) {
	println("Running!")
	if !c.Bool("testMode") {
		println("service mode")
		service()
	} else {
		println("test mode")
		uri := "amqp://guest:guest@" + c.String("server") + ":5672"
		if c.Int("consumer") > -1 {
			makeConsumers(uri, c.Int("concurrency"), c.Int("consumer"))
		}

		//	if c.Int("producer") != 0 {
		//		makeProducers(c.Int("producer"), c.Int("wait"), c.Int("concurrency"))
		//	}
	}
}

func MakeQueue(c *amqp.Channel) amqp.Queue {
	q, err2 := c.QueueDeclare("stress-test-exchange", true, false, false, false, nil)
	if err2 != nil {
		panic(err2)
	}
	return q
}

//func makeProducers(n int, wait int, concurrency int) {
//
//	taskChan := make(chan int)
//	for i := 0; i < concurrency; i++ {
//		go Produce(config, taskChan)
//	}
//
//	start := time.Now()
//
//	for i := 0; i < n; i++ {
//		taskChan <- i
//		time.Sleep(time.Duration(int64(wait)))
//	}
//
//	time.Sleep(time.Duration(10000))
//
//	close(taskChan)
//
//	log.Printf("Finished: %s", time.Since(start))
//}

func makeConsumers(uri string, concurrency int, toConsume int) {
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
