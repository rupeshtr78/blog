---
layout: post
title: Flink Streaming
tags: [flink,streaming,scala,jekyll]
image: '/images/flink/flink-main.png'
---

- Flink is a stream processing engine. Flink can run on standalone cluster, or on top of YARN or Mesos
- Flink is scalable (1000’s of nodes),Fault-tolerant survive failures while still guaranteeing exactly-once processing
- Flink has good Scala support, similar to Spark Streaming
- Flink can process data based on event times,windowing system
- Flink supports real-time streaming 
  

In this example we will create app to read from socket stream and write the word count using Flink.

To be able to run Flink, the only requirement is to have a working **Java 8 or 11** installation.

Folowing is an example of Run flink socket streaming word count application from intellij IDE.

##### Scala Code

```scala
    // set up the execution environment
    val conf: Configuration = new Configuration()
    val env =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


```

```scala
 // get socket  data
    val text = env.socketTextStream("XXX.XXX.1.XXX",1900)

    val counts: DataStream[(String, Int)] = text
      .flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // print result
    counts.print()


    // lazy execution
    env.execute("Flink Streaming")

```

Flink WebUI Can be accessed from http://localhost:8081

Jobs Section shows job details with DAG

![](/images/flink/jobs-dag.png)

![](/images/flink/jobs-timeline.png)

Task Manager Details

![](/images/flink/teask-manager.png)



Results are printed to Console

![](/images/flink/intellij.png)



##### Dependencies

```scala
  "org.apache.flink" %% "flink-scala" % flinkVersion ,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion ,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion,
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion
```

