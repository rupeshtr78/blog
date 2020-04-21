---
layout: post
title: Kafka Cluster
tags: [kafka,cluster,mongodb]
image: '/images/kafka/Kafka-zk.png'
---

> - Kafka is simply a collection of topics split into one or more partitions. 
> - A Kafka partition is a linearly ordered sequence of messages, where each message is identified by their index (offset).
> - Kafka broker leader election can be done by ZooKeeper.ZooKeeper is used for managing and coordinating Kafka broker. 
> - Zookeeper serves as the coordination interface between the Kafka brokers and consumers
>
> ###### Topics :
>
> - A Topic is a category/feed name to which messages are stored and published.
> - All Kafka messages are organized into topics.
> - We can send a message/record to specific topic and we can read a message/data from the topic name.
> - Producer applications **write data to topics** and consumer applications **read from topics**.
> - Published messages will be stay in the kafka cluster until a configurable retention period has passed by.
>
> ###### Producers :
>
> - The application which sends the messages to kafka system.
> - The published data will send to specific topic.
> - A producer, which enables the publication of records and data to topics.
>
> ###### Consumers :
>
> - A consumer, which reads messages and data from topics.
> - The application which read/consume the data from the specific topic in the kafka system.
>
> ###### Broker :
>
> - Every instance of Kafka that is responsible for message exchange is called a Broker
> - Kafka can be used as a stand-alone machine or a part of a cluster.
> - Kafka brokers are stateless, so they use ZooKeeper for maintaining their cluster state
> - One Kafka broker instance can handle hundreds of thousands of reads and writes per second
> - Kafka broker leader election can be done by ZooKeeper.
>
> ###### Zookeeper
>
> - ZooKeeper is used for managing and coordinating Kafka broker, it service is mainly used to notify producer and consumer about the presence of any new broker in the Kafka system or failure of the broker in the Kafka system. 

##### Start Scrips for Kafka Zookeeper

```shell
### Start Zookeeper on each node
kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties &
### Start in deamon mode
kafka/bin/zookeeper-server-start.sh -daemon kafka/config/zookeeper.properties

### Start Kafka Brokers on each node
kafka/bin/kafka-server-start.sh kafka/config/server.properties
### Start in deamon mode
kafka/bin/kafka-server-start.sh -daemon kafka/config/server.properties


```

##### Kafka Topics

##### Create the input topic
```shell
  kafka/bin/kafka-topics.sh  --create --zookeeper zookeeperip:2181 \
          --replication-factor 1 --partitions 1 \
          --topic kafka-streams-input
```




##### List Topics
```shell
./bin/kafka-topics.sh --zookeeper zookeeperip:2181 --list
```



##### Producer Pipe Input Files
```shell
cat /data/file-input.txt | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-plaintext-input
./bin/kafka-console-producer.sh --broker-list zookeeperip:9092 --topic rtr-spark-topic
< data/access_log.txt
```



##### Consumer and Output to terminal
```shell
./bin/kafka-console-consumer.sh --bootstrap-server kafkabrokerip:9092 \
        --topic kafka-streams-input \
        --from-beginning 


```

