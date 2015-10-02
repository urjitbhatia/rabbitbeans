package beans

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

type TestBeanHandler struct {
	NumToProduce int
	WaitPerBean  int
}

func (me *TestBeanHandler) WriteToBeanstalkd(c <-chan amqp.Delivery) {
	for n := 0; n < me.NumToProduce; n++ {
		msg := <-c
		msg.Ack(
			false, // no multiple Acks
		)
	}
}
func (me *TestBeanHandler) ReadFromBeanstalkd(c chan<- Bean) {

	log.Println("Waiting for", me.WaitPerBean)
	for i := 0; i < me.NumToProduce; i++ {
		c <- Bean{
			uint64(i),
			[]byte("test bean"),
			func() {
			},
			func() {
			},
		}
		time.Sleep(time.Duration(me.WaitPerBean) * time.Millisecond)
	}
}
