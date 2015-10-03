package rabbitbeans

import (
	"log"
	"time"
)

/*
 * Job is a unit of work that is shuttled between Rabbit and Beanstalkd.
 */
type Job struct {
	Id   uint64
	Body []byte // Actual data
	//	Ack         func()
	//	Nack        func()
	Acknowledger
	ContentType string        // Data type. Example: application/json, application/text etc
	Priority    uint32        // Priority < 2**32 most urgent priority is 0; least urgent priority is 4,294,967,295
	Delay       time.Duration // Run after Delay seconds
	TTR         time.Duration // Time to run
	ExternalId  string        // External custom id, if any. String to keep type flexible. Has to be unique
}

// An Ackowledger knows how to ack/nack anything that it is linked to.
type Acknowledger interface {
	Ack(id uint64) error
	Nack(id uint64) error
}

// A single pipe component that processes items
type Pipe interface {
	Process(in chan interface{}) chan interface{}
}

// A pipeline composed of multiple pipes
type Pipeline struct {
	head chan interface{}
	tail chan interface{}
}

func (p *Pipeline) Enqueue(item interface{}) {
	p.head <- item
}

func (p *Pipeline) Dequeue(c chan interface{}) {
	for i := range p.tail {
		c <- i
	}
}

//func (p *Pipeline) Dequeue(handler func(interface{})) {
//	for i := range p.tail {
//		handler(i)
//	}
//}

func (p *Pipeline) Close() {
	close(p.head)
}

// Pipeline maker takes multiple pipes in order and connects them
func NewPipeline(pipes ...Pipe) Pipeline {
	log.Printf("1")
	head := make(chan interface{})
	log.Printf("2")
	var next_chan chan interface{}
	log.Printf("3")
	for _, pipe := range pipes {
		log.Printf("4")
		if next_chan == nil {
			next_chan = pipe.Process(head)
		} else {
			next_chan = pipe.Process(next_chan)
		}
	}
	log.Printf("4")
	return Pipeline{head: head, tail: next_chan}
}
