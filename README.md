#Streamer
Stream data to a distributed cluster environment.

i.e Price feeds or data messages,

Idea is to publish to a distributed cluster and subscribe from any node.

Two implementations, Kafa and Redis.
##Redis,
Low latency, message broker (For price feeds)
Use Kafka for any messages where latency of +-150ms is acceptable.

##Kafka
Can keep a lot of data, big messages, very robust 