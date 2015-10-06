package main

import (
	"encoding/json"
	"errors"
	"github.com/codegangsta/cli"
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
	RabbitConfig rabbit.Config
	BeansConfig  beans.Config
}

const (
	defaultRabbitToBean = false
)

func service() {
	config := configure()
	// The channel that takes in rabbits (rabbit jobs) and delivers them to beans (beanstalkd)
	pipeline := rabbitbeans.NewPipeline(createRedisPipe(RedisConfig{}))

	var waitGroup sync.WaitGroup
	if config.RabbitToBean {
		jobs := make(chan interface{})

		waitGroup.Add(1)
		ReadFromRabbit(config.RabbitConfig, waitGroup, jobs)

		waitGroup.Add(1)
		WriteToBeanstalkd(config.BeansConfig, waitGroup, jobs)
	} else {
		jobs := make(chan interface{})

		waitGroup.Add(1)
		ReadFromBeanstalkd(config.BeansConfig, waitGroup, jobs)
		go attachPipeSource(pipeline, jobs)

		sink := make(chan interface{})

		go attachPipeSink(pipeline, sink)
		waitGroup.Add(1)
		WriteToRabbit(config.RabbitConfig, waitGroup, sink)
	}

	waitGroup.Wait()
}

func createRedisPipe(redisConfig RedisConfig) rabbitbeans.Pipe {
	redisPipe, err := NewRedisPipe(redisConfig)
	rabbitbeans.FailOnError(err, "Can't connect to redis")
	return redisPipe
}

func attachPipeSource(pipeline rabbitbeans.Pipeline, source chan interface{}) {
	log.Printf("Attaching source to pipe")
	for msg := range source {
		pipeline.Enqueue(msg)
	}
}
func attachPipeSink(pipeline rabbitbeans.Pipeline, sink chan interface{}) {
	pipeline.Dequeue(sink)
	//	pipeline.Dequeue(func(i interface{}) {
	//		if s, ok := i.(rabbitbeans.Job); !ok {
	//			rabbitbeans.LogOnError(errors.New("Cannot cast"), "Error")
	//		} else {
	//			sink <- rabbitbeans.Job(s)
	//		}
	//	})
}

func ReadFromRabbit(config rabbit.Config, waitGroup sync.WaitGroup, jobs chan<- interface{}) {
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	go func() {
		defer waitGroup.Done()
		rabbitConn.ReadFromRabbit(queueName, jobs)
	}()
}

func WriteToRabbit(config rabbit.Config, waitGroup sync.WaitGroup, jobs <-chan interface{}) {
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	go func() {
		defer waitGroup.Done()
		rabbitConn.WriteToRabbit(queueName, jobs)
	}()
}

func ReadFromBeanstalkd(config beans.Config, waitGroup sync.WaitGroup, jobs chan<- interface{}) {
	go func() {
		defer waitGroup.Done()
		beansConn := beans.Dial(config)
		beansConn.ReadFromBeanstalkd(jobs)
	}()
}

func WriteToBeanstalkd(config beans.Config, waitGroup sync.WaitGroup, jobs <-chan interface{}) {
	go func() {
		defer waitGroup.Done()
		beansConn := beans.Dial(config)
		beansConn.WriteToBeanstalkd(jobs)
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

func main() {
	app := cli.NewApp()
	app.Name = "tester"
	app.Usage = "Make the rabbit cry"
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "server, s", Value: "localhost", Usage: "Hostname for RabbitMQ server"},
		cli.IntFlag{Name: "RabbitWrite, rw", Value: 0, Usage: "Number of messages to send from stub beanstalkd to Rabbit"},
		cli.IntFlag{Name: "RabbitRead, rr", Value: 0, Usage: "Number of messages to read from Rabbit to stub beanstalkd"},
		cli.IntFlag{Name: "BeanWrite, bw", Value: 0, Usage: "Number of messages to send from stub rabbit to beanstalkd"},
		cli.IntFlag{Name: "BeanRead, br", Value: 0, Usage: "Number of messages to send from beanstalkd to stub rabbit"},
		cli.IntFlag{Name: "concurrency, c", Value: 1, Usage: "number of beanToRabbit/rabbitToBeans Goroutines"},
		cli.IntFlag{Name: "wait, w", Value: 0, Usage: "Number of milliseconds to wait between publish events"},
		cli.BoolFlag{Name: "testMode, t", Usage: "Run stress test mode. Runs as a service otherwise (default)"},
	}
	app.Action = func(c *cli.Context) {
		RunApp(c)
	}
	app.Run(os.Args)
}
