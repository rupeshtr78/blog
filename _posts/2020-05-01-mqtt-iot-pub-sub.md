---
layout: post
title: MQTT Publish Subscribe IOT Part 1
tags: [iot,mqtt,spark,cassandra,broker,gpio,python,jekyll]
image: '/images/mqtt/mqtt-spark.png'
---

We will create a  IOT Device / MQTT Client using Raspberry Pi. With a simple push button switch to simulate an event using bread board connected to the Pi device.

In the event of pressing button the device will publish access logs ( In our case we will read from a file line by line) to the broker. Later we will create a Spark Streaming application which will read the published topic and related messages into spark stream .Using Spark we will parse the log data ,count the status field using Spark window functions and watermark. Finally we will write the stream of data to Cassandra sink.

MQTT (Message Queue Telemetry Transport) is a lightweight messaging protocol which is ideal for communication of IoT connected devices. 

MQTT has three components: broker, publisher, and subscriber. 

- A broker is an intermediary entity that handles the communication going on between devices. 
- A publisher is a device that sends messages. 
- A subscriber listens to the messages sent by the publisher.
- A topic is necessary for communication between different devices.

The **MQTT** protocol is based on the principle of **publishing** messages and **subscribing** to **topics**.

Device Clients connect to the broker and **publish** messages to **topics**.

Subscribe clients can Connect to a **broker** and **subscribe** to **topics** that they are interested in. 

In this example We will use the popular broker Mosquitto.



**BreadBoard Circuit**

![]({{ site.baseurl }}/images/mqtt/rpi-nice-01-png.png)

Configure Mosquitto MQTT Broker

Configure the mosquitto.conf  file.

```sh
port 1883
listener 8883 192.168.1.200
persistence true
persistence_location /home/hyper/mosquitto/var/lib/mosquitto/
persistence_file mosquitto.db
log_dest syslog
log_dest stdout
log_dest topic
log_type error
log_type warning
log_type notice
log_type information
connection_messages true
log_timestamp true
allow_anonymous true
```



On the Pi device create the Mqtt Client using paho.mqtt package.

```sh
pip install paho-mqtt
```

We will create mqttClient Package which takes broker, port , message and topic as parameters.

```python
import paho.mqtt.client as mqtt
import time

def on_log(client,userdata,level,buf):
    print("log: ",buf)

def on_connect(client,userdata,level,rc):
    if rc == 0:
        print("Connected "+str(rc))
        # client.subscribe("$SYS/#")
    else:
        print("Bad Connection Verify Broker "+str(rc))

def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    

def on_disconnect(client,userdata,level,rc=0):
    print("Client Disconnected "+str(rc))

def main(broker, port,topic , message):
  
    client = mqtt.Client("GPIOMQTT01")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_log = on_log

    client.connect(broker,port,60)
    client.loop_start()

    # client.subscribe(topic)
    client.publish(topic , message)


    time.sleep(.3)
    client.loop_stop()
    # client.disconnect()
    # client.loop_forever()

if __name__ == '__main__':
    main()
```

We will use the RPI.GPIO package which provides a class to control the *GPIO* on a *Raspberry Pi*.

In the event of pressing button we will call the method mqttPublishLines to read lines from a file and publish to the broker.

```python
GPIO.add_event_detect(buttonPin,GPIO.BOTH,callback=mqttPublishLines,bouncetime=300)
```

In the payload we will add the current timestamp and the access logs by reading from a file line by line each time the button is pressed.

Later we can subscribe to this topic , utilize the the timestamp field for Spark window functions and perform some aggregations on streaming data.

```python
payload = {"iottimestamp":str(datetime.utcnow()), "accesslogs":line[count]}
```

We will encode the payload into json.

```python
message = json.dumps(payload)
```

Finally call the mqtt client publish the message to the broker and topic.

```python
 mqttClient.main(broker, port, topic, message)
```

Read input from GPIO and Publish MQTT message.

```python
import RPi.GPIO as GPIO
import time
import mqttClient
import json
from datetime import datetime



lightPin = 4
buttonPin = 17

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)

lightPin = 4
buttonPin = 17
GPIO.setup(lightPin, GPIO.OUT,initial=GPIO.LOW)
GPIO.setup(buttonPin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

def mqttPublishLines(buttonPin):
    topic = "rupesh/gpiotopic"
    broker = "192.168.1.200"
    port = 8883
    filepath = "/home/pi/mqtt/data/access_log.txt"

    file = open(filepath, "r")
    line = file.readlines()
    count = 0

    while True:
        if GPIO.input(buttonPin)==GPIO.LOW:
            time.sleep(0.1)
            count +=1
            payload = {"iottimestamp":str(datetime.utcnow()), "accesslogs":line[count]}
            message = json.dumps(payload)
            print("Button Pressed", GPIO.input(buttonPin))
            print("Event Detected", GPIO.event_detected(buttonPin))
            GPIO.output(lightPin, True)
            mqttClient.main(broker, port, topic, message)
        else:
            GPIO.output(lightPin, False)


GPIO.add_event_detect(buttonPin,GPIO.BOTH,callback=mqttPublishLines,bouncetime=300)

raw_input("Press Enter To Quit\n>")

try:
    print("Keyboard Interrupted!")
finally:
    GPIO.cleanup()


```

At this point lets test our setup. By pressing the Button we should be able send access logs to the broker.

To verify the setup we will subscribe to this topic from CLI at this point.

**Start the Broker** on the MQTT Broker Server.

```sh
mosquitto -c  mosquitto.conf
```

![]({{ site.baseurl }}/images/mqtt/broker-cli.png)

**Start the MQTT Client** on Raspberry Pi device

```sh
python gpioMqttLinesJson.py
```

![]({{ site.baseurl }}/images/mqtt/pi-button-pressed.png)

**Subscribe to the topic** on Borker Server CLI

```sh
mosquitto_sub -h 192.168.1.200 -p 8883 -d -t "rupesh/gpiotopic"
```

![]({{ site.baseurl }}/images/mqtt/subscribe-cli.png)

When we press the button the We can see the message access logs has been published to the topic rupesh/gpiotopic with timestamp



Source Code for this exercise can be found [here](https://github.com/rupeshtr78/mqttspark).



