package rabbitbeans

/*
 * Pipeline connects multiple pipes in order.
 */
type Pipeline struct {
	head chan interface{}
	tail chan interface{}
}

/*
 * Enqueue method takes a message and adds it to
 * the start of the pipeline.
 */
func (p *Pipeline) Enqueue(item interface{}) {
	p.head <- item
}

/*
 * Dequeue method takes a terminating channel and dequeues the messages
 * from the pipeline to that channel.
 */
func (p *Pipeline) Dequeue(c chan interface{}) {
	for i := range p.tail {
		c <- i
	}
}

/*
 * Close makes sure that the pipeline accepts no further messages
 */
func (p *Pipeline) Close() {
	close(p.head)
}

/*
 * NewPipeline takes multiple pipes in-order and connects them to form a pipeline.
 * Enqueue and Dequeue methods are used to attach source/sink to the pipeline.
 */
func NewPipeline(pipes ...Pipe) Pipeline {
	head := make(chan interface{})
	var next_chan chan interface{}
	for _, pipe := range pipes {
		if next_chan == nil {
			next_chan = pipe.Process(head)
		} else {
			next_chan = pipe.Process(next_chan)
		}
	}
	return Pipeline{head: head, tail: next_chan}
}
