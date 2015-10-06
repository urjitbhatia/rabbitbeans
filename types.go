package rabbitbeans

import (
	"time"
)

/*
 * Job is a unit of work that is shuttled between Rabbit and Beanstalkd.
 */
type Job struct {
	Id   uint64
	Body []byte // Actual data
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
