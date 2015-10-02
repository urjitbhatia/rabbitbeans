package rabbit

import (
	"github.com/streadway/amqp"
	"github.com/urjitbhatia/rabbitbeans/beans"
	"log"
	"time"
)

type TestRabbitHandler struct {
	NumToProduce  int
	WaitPerRabbit int
}

func (*TestRabbitHandler) WriteToRabbit(c <-chan beans.Bean) {
	<-c
}
func (me *TestRabbitHandler) ReadFromRabbit(c chan<- amqp.Delivery) {

	log.Println("Waiting for", me.WaitPerRabbit)
	for i := 0; i < me.NumToProduce; i++ {
		d := amqp.Delivery{}
		d.Body = []byte("test rabbit")
		c <- d
		time.Sleep(time.Duration(me.WaitPerRabbit) * time.Millisecond)
	}
}
