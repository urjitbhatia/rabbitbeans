package rabbit

import (
	"errors"
	"github.com/urjitbhatia/rabbitbeans"
	"log"
	"time"
)

type TestRabbitHandler struct {
	NumToProduce  int
	WaitPerRabbit int
}

type FakeBeanstalkdAcknowledger struct{}

func (FakeBeanstalkdAcknowledger) Ack(id uint64) error  { return nil }
func (FakeBeanstalkdAcknowledger) Nack(id uint64) error { return nil }

func (me *TestRabbitHandler) WriteToRabbit(c <-chan interface{}) {
	for n := 0; n < me.NumToProduce; n++ {
		j := <-c
		job, ok := j.(rabbitbeans.Job)
		if !ok {
			rabbitbeans.FailOnError(errors.New("Unknown message on channel"), "Can't put message on rabbit")
		}
		job.Ack(job.Id)
	}
}
func (me *TestRabbitHandler) ReadFromRabbit(c chan<- interface{}) {

	log.Println("Waiting for", me.WaitPerRabbit)
	for i := 0; i < me.NumToProduce; i++ {
		d := rabbitbeans.Job{}
		d.Body = []byte("test rabbit")
		d.Acknowledger = &FakeBeanstalkdAcknowledger{}
		d.TTR = time.Duration(100) * time.Minute
		c <- d
		time.Sleep(time.Duration(me.WaitPerRabbit) * time.Millisecond)
	}
}
