package beans

import (
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

func (me *TestBeanHandler) WriteToBeanstalkd(c <-chan rabbitbeans.Job) {
	for n := 0; n < me.NumToProduce; n++ {
		msg := <-c
		msg.Ack(0)
	}
}
func (me *TestBeanHandler) ReadFromBeanstalkd(c chan<- rabbitbeans.Job) {

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
