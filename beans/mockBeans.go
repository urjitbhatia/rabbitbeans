package beans

import (
	"errors"
	"github.com/urjitbhatia/rabbitbeans"
	"log"
	"time"
)

type TestBeanHandler struct {
	NumToProduce int
	WaitPerBean  int
}

type FakeBeanstalkdAcknowledger struct{}

func (FakeBeanstalkdAcknowledger) Ack(id uint64) error  { return nil }
func (FakeBeanstalkdAcknowledger) Nack(id uint64) error { return nil }

func (me *TestBeanHandler) WriteToBeanstalkd(c <-chan interface{}) {
	for n := 0; n < me.NumToProduce; n++ {
		m := <-c
		msg, ok := m.(rabbitbeans.Job)
		if !ok {
			rabbitbeans.FailOnError(errors.New("Unknown message on channel"), "Can't put job on beanstalkd")
		}
		msg.Ack(0)
	}
}
func (me *TestBeanHandler) ReadFromBeanstalkd(c chan<- interface{}) {

	log.Println("Waiting for", me.WaitPerBean)
	for i := 0; i < me.NumToProduce; i++ {
		c <- rabbitbeans.Job{
			uint64(i),
			[]byte("test bean"),
			&FakeBeanstalkdAcknowledger{},
			"", 0, 0, 0, "",
		}
		time.Sleep(time.Duration(me.WaitPerBean) * time.Millisecond)
	}
}
