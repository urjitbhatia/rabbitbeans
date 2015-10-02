# rabbitbeans
A simple router to consume messages from rabbitmq and push to beanstalkd and vice-versa.
It can connect to a remote/local beanstalkd/rabbitmq based on the configuration.

[![GoDoc](https://godoc.org/github.com/urjitbhatia/rabbitbeans?status.svg)](https://godoc.org/github.com/urjitbhatia/rabbitbeans)

# design goals

- Beanstalkd provides a nice way of scheduling jobs (with delays, TTR semantics etc) while RabbitMQ provides a HA queuing system that is distributed, fault tolerant, high performance, and persistent store for job payloads.
- Combining the two, we can architect a system that puts job payloads on (single/multiple) rabbitQueues which provides an HA endpoint to pump jobs to. RabbitBeans listens on the rabbitQueues (which also provide a buffer to smooth the downstream throughput) and creates jobs on Beanstalkd (potentially multiple).
- In the reverse direction, it can also listen for ready jobs on Beanstalkd and route them to Rabbit queues.

```
                             +---------+                             
+----------------+  Queue    |         |    Tube   +----------------+
| RabbitMQ       +<<---------+         +<<---------+  Beanstalkd    |
|                |           |         |           |                |
+----------------+           |         |           +----------------+
                             |  Rabbit |                             
+----------------+  Queue    |  Beans  |    Tube   +----------------+
| RabbitMQ       +--------->>+         +--------->>+  Beanstalkd    |
|                |           |         |           |                |
+----------------+           +---------+           +----------------+
```

# todo

- Provide a way to cancel jobs
- Post metrics
- Improve benchmark tests
THIS IS STILL UNDER EARLY DEVELOPMENT - DO NOT USE IN PRODUCTION...

# acknowledgements

Thanks to: https://github.com/backstop/rabbit-mq-stress-tester for the inspiration behind the benchmarking code
