---
layout: post
title: Custom MQTT Spark Receiver
tags: [iot,aws,spark,scale,streaming,mqtt,jekyll]
image: '/images/mqtt/SparkReceiver.png'
---

- Spark Streaming’s **Receivers** accept data in parallel and **buffer it in the memory** of Spark’s workers nodes. 
- Each Input **DStream** is associated with a **Receiver** object which receives the data from a source and stores it in Spark’s memory for processing.
- Then the latency-optimized Spark engine runs short tasks (tens of milliseconds) to **process the batches and output the results** to other systems. 
- Spark **tasks** are assigned dynamically to the workers based on the locality of the data and available resources. This enables both better load balancing and faster fault recoveryDividing the data into **small micro-batches** allows for fine-grained allocation of computations to resources.
- The key programming abstraction in Spark Streaming is a **DStream**, or distributed stream. Each **batch** of streaming data is represented by an **RDD**, which is Spark’s concept for a distributed dataset. Therefore a DStream is just a series of RDDs. This common representation allows batch and streaming workloads to interoperate seamlessly
- Abstract class of a receiver that can be run on worker nodes to receive external data. 
   * A custom receiver can be defined by defining the functions `onStart()` and `onStop()`. 
   * `onStart()`should define the setup steps necessary to start receiving data,
   * `onStop()` should define the cleanup steps necessary to stop receiving data.Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
   * Exceptions while receiving can be handled either by restarting the receiver with `restart(...)`
   * or stopped completely by `stop(...)`.

```scala
package mqtt.receiver

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{IMqttDeliveryToken, MqttAsyncClient, MqttCallback, MqttClient, MqttException, MqttMessage}

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

abstract class MqttReceiver[T](brokerURL:String,topic:String)
  extends Receiver[T](StorageLevel.MEMORY_AND_DISK_2){


  def processMqttMessages(payload: Array[Byte])

  val mqttPromise = Promise[MqttClient]()
  val mqttFuture = mqttPromise.future

  override def onStart(): Unit = {

    val persistence:MemoryPersistence = new MemoryPersistence()
    val mqttClient = "SparkMqttClient"     //MqttClient.generateClientId()
    val client:MqttClient = new MqttClient(this.brokerURL,mqttClient,persistence)

    mqttPromise.success(client)

    val callBack:MqttCallback = new MqttCallback {
      override def connectionLost(cause: Throwable): Unit = {
        restart("Custom Mqtt Receiver Restarting Connection Lost" ,cause)

      }

      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        processMqttMessages(message.getPayload)
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
        println("Got delivery token ")
      }

    }

    client.setCallback(callBack)

    try client.connect()
    catch {
      case err: MqttException =>
        println("Client Connect Error " ,err.getMessage)
        err.printStackTrace()
    }

    try {
      client.subscribe(topic,1)
    } catch {
      case err: MqttException =>
        println("Client Subscribe Error", err.getMessage)
        err.printStackTrace()

    }
  }

  override def onStop(): Unit = {
    println("Mqtt Receiver Stop")
    mqttFuture.foreach((client => client.close()))
  }
}


class MqttStringReceiver(brokerURL:String,topic:String)
  extends MqttReceiver[String](brokerURL:String ,topic:String){
  override def processMqttMessages(payload: Array[Byte]): Unit = {
    // run on another thread
    Future {
      val iterator = Source.fromBytes(payload).getLines()
      while(!isStopped && iterator.hasNext) {
       /** Store an iterator of received data as a data block into Spark's memory. */
        store(iterator.next)
      }
//        .getLines()
//        .foreach(line => store(line))
    }

  }
}
```



Read From Mosquitto MQTT Broker/Server

Start Mosquitto MQTT Server : 

```
 mosquitto -c  mosquitto.conf
```



```scala
  def readMqttStream:DStream[String] = {
    val brokerURL = "tcp://192.XXX.1.XXX:1883"
    val topic = "rupesh/gpiotopic"
    val dstream: DStream[String] = ssc.receiverStream(
        new MqttStringReceiver(brokerURL,topic))
    dstream

  }
```



