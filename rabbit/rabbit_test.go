package rabbit

import (
	. "github.com/onsi/ginkgo"
	"github.com/streadway/amqp"
	//	rabbit "github.com/urjitbhatia/rabbitbeans/rabbit"
	"log"
)

type mockAmqp struct {
}

func (me *mockAmqp) Channel() (*amqp.Channel, error) {
	log.Println("mocked channel")
	ch := &amqp.Channel{}
	ch.QueueInspect = func(queueName string) (amqp.Queue, error) {
		return amqp.Queue{}, nil
	}
	return ch, nil
}

var _ = Describe("Rabbit", func() {
	var (
	//		mockRabbit         rabbit.Connection
	//		mockAmqpConnection = &mockAmqp{}
	//		config = &rabbit.Config{}
	)

	//	BeforeEach(func() {
	//	})

	Describe("loading from JSON", func() {
		Context("when the JSON parses succesfully", func() {
			It("should populate the fields correctly", func() {
				connection := Connection{}
				mockRabbit := &mockAmqp{}
				connection.rabbitConnection = mockRabbit
				jobs := make(chan amqp.Delivery)
				connection.Consume("testQueue", jobs)
			})
		})
	})
})
