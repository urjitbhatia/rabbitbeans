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
	for i := 0; i < me.NumToProduce; i++ {
		c <- Bean{
			uint64(i),
			[]byte("test bean"),
			func() {
				log.Println("Job acked...")
			},
			func() {
				log.Println("Job nacked...")
			},
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)
	}
}
