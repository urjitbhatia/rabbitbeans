package rabbit

import (
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

func (me *TestRabbitHandler) WriteToRabbit(c <-chan rabbitbeans.Job) {
	for n := 0; n < me.NumToProduce; n++ {
		job := <-c
		job.Ack(job.Id)
	}
}
func (me *TestRabbitHandler) ReadFromRabbit(c chan<- rabbitbeans.Job) {

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
