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
		ReadFromRabbit(waitGroup, jobs)

		waitGroup.Add(1)
		WriteToBeanstalkd(waitGroup, jobs)
	} else {
		jobs := make(chan beans.Bean)

		waitGroup.Add(1)
		ReadFromBeanstalkd(waitGroup, jobs)

		waitGroup.Add(1)
		WriteToRabbit(waitGroup, jobs)
	}

	waitGroup.Wait()
}

func ReadFromRabbit(waitGroup sync.WaitGroup, jobs chan<- amqp.Delivery) {
	config := rabbit.Config{}
	config.Quiet = true
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	go func() {
		defer waitGroup.Done()
		rabbitConn.ReadFromRabbit(queueName, jobs)
	}()
}

func WriteToRabbit(waitGroup sync.WaitGroup, jobs <-chan beans.Bean) {
	config := rabbit.Config{}
	config.Quiet = true
	rabbitConn := rabbit.Dial(config)
	queueName := "scheduler"
	go func() {
		defer waitGroup.Done()
		rabbitConn.WriteToRabbit(queueName, jobs)
	}()
}

func ReadFromBeanstalkd(waitGroup sync.WaitGroup, jobs chan<- beans.Bean) {
	go func() {
		defer waitGroup.Done()
		config := beans.Config{
			"127.0.0.1",
			"11300",
			2,
			true,
		}
		beansConn := beans.Dial(config)
		beansConn.ReadFromBeanstalkd(jobs)
	}()
}

func WriteToBeanstalkd(waitGroup sync.WaitGroup, jobs <-chan amqp.Delivery) {
	go func() {
		defer waitGroup.Done()
		config := beans.Config{
			"127.0.0.1",
			"11300",
			0,
			true,
		}
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
		cli.IntFlag{Name: "RabbitWrite, rw", Value: 0, Usage: "Number of messages to send from Mock beanstalkd to Rabbit"},
		cli.IntFlag{Name: "RabbitRead, rr", Value: 0, Usage: "Number of messages to send from Mock beanstalkd to Rabbit"},
		cli.IntFlag{Name: "BeanWrite, bw", Value: 0, Usage: "Number of messages to send from mock rabbit to beanstalkd"},
		cli.IntFlag{Name: "BeanRead, br", Value: 0, Usage: "Number of messages to send from mock rabbit to beanstalkd"},
		cli.IntFlag{Name: "concurrency, c", Value: 1, Usage: "number of beanToRabbit/rabbitToBeans Goroutines"},
		cli.IntFlag{Name: "wait, w", Value: 0, Usage: "Number of milliseconds to wait between publish events"},
		//		cli.IntFlag{Name: "bytes, b", Value: 0, Usage: "number of extra bytes to add to the RabbitMQ message payload. About 50K max"},
		cli.BoolFlag{Name: "testMode, t", Usage: "Run stress test mode. Runs as a service otherwise (default)"},
	}
	app.Action = func(c *cli.Context) {
		RunApp(c)
	}
	app.Run(os.Args)
}
