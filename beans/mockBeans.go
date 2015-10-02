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

func (*TestBeanHandler) Publish(c <-chan amqp.Delivery) {
	<-c
}
func (me *TestBeanHandler) Consume(c chan<- Bean) {

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
