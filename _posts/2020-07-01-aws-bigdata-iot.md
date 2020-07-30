---
layout: post
title: AWS Big Data IOT
tags: [iot,aws,spark,kinesis,redshift,emr,DynamoDB,jekyll]
image: '/images/awsbigdata/awsbigdata.png'
---

Business case :Use AWS Big data services to process streaming data from IOT device.

An Iot device will publish the license plate and clocked speed of vehicles as data streams into AWS for further processing.

AWS Spark job will filter the cars above speed limit ,Look up the vehicle owner information stored in Redshift data warehouse and eventually sends a SNS text message alert to the registered owner of the vehicle in real time.

AWS Services Used in this exercise.

- AWS IOT Core
- Kinesis Streams,  Firehose, Analytics
- AWS RedShift
- AWS EMR
- AWS Lambda
- AWS SNS
- AWS Glue ETL , Crawler
- Spark
- DynamoDB , DynamoDB Streams
- EC2

**IOT Data Flow**

1. Simulate the IOT device using a raspberry pi .Register the device with AWS IOT Core. 
2. On the IOT device when a button is pressed on breadboard will do a callback function to create a MQTT publish which sends the license plate and speed to AWS IOT Gateway.(NO image capturing mechanism publish script will read from a file and publish the data(licensePlate, longitude, latitude,city,state,speed).
3. Define IOT Rules in AWS IOT core to send the data to Kinesis Stream
4. AWS Emr Spark cluster will read the data from Kinesis Stream.
   1. Filter the speeding cars ,
   2. Lookup the driver information from Redshift  based on license plate and 
   3. Put records to outgoing kinesis stream with driver name and phone number information.
   4. Raw data will be written to a DynamoDB Table.
5. AWS Lambda function will read this streaming data from Kinesis and publish to AWS SNS topic 
6. SNS will send the SMS message to the phone number of the owner registered with the license plate.

![SMS Message From AWS]({{ site.baseurl }}/images/awsbigdata/snsmessage.png)

**Redshift Data Flow**

1. We will use AWS Kinesis agent to read and the driver data from csv and put records to Kinesis Firehose. (first_name, last_name, licensePlate, email,address, city,state,zipcode,phoneNumber,myphone) 
2. Use Kinesis Firehose to load the data to S3 Bucket
3. Use AWS Glue crawler , Glue ETL Job to load the data from S3 into Redshift Cluster using jdbc.
4. The vehicle owner data in Redshift will be joined by the license plate from streaming data of speeding vehicles by AWS EMR Spark .

![]({{ site.baseurl }}/images/awsbigdata/redshit-query-results-raw.png)

**IOT Data Flow**

**Register IOT Thing in AWS IOT Core service**

1. Service AWS IOT Core  -> Onboard
2. Register a device in AWS IoT registry. IOT device needs a thing record in order to work with AWS IoT.
3. Download a connection kit. Security credentials(.pem ,.key files), the SDK of your choice.(Python)
4. Upload the connection kit zip file to device .
6. Unzip connect_device_package.zip
7. ./start.sh
8. Subscribe the message topic under the test section in IOT core verify the communication with device. See the messages published under test section in IOT core.
9. Create your publish script and run the below command to run myPubSub.py which reads from a text file and send a **json** payload to the specified topic using MQTT publish subscribe protocol.

```python
 def mqttPublishLines(buttonPin):
     publish_count = 1
     filepath = "/home/pi/awsiot/mqtt/data/speeding_data.csv"
     file = open(filepath, "r")
     #line = file.readlines()
     line = file.read().splitlines()
     while (publish_count <= args.count) or (args.count == 0):
         if GPIO.input(buttonPin)==GPIO.LOW:
             payload = {"iottimestamp":str(datetime.utcnow()),
                         "licensePlate":line[publish_count].split(",")[0],
                         "longitude":line[publish_count].split(",")[1],
                         "latitude":line[publish_count].split(",")[2],
                         "city":line[publish_count].split(",")[3],
                         "state":line[publish_count].split(",")[4],
                         "speed":line[publish_count].split(",")[5] }
             message = json.dumps(payload)
             print("Button Pressed", publish_count)
             GPIO.output(lightPin, True)
             mqtt_connection.publish(
             topic=args.topic,
             payload=message,
             qos=mqtt.QoS.AT_LEAST_ONCE)
             time.sleep(1)
             publish_count += 1
         else:
             GPIO.output(lightPin, False)

 GPIO.add_event_detect(buttonPin,GPIO.BOTH,callback=mqttPublishLines,bouncetime=300)
```

Run Publish python script from IOT Device.

```shell
python iotPubSub.py --endpoint xxxxxxx.iot.us-east-2.amazonaws.com --root-ca root-CA.crt --cert RTRPIIOT.cert.pem --key RTRPIIOT.private.key --client-id basicPubSub --topic rtr/iot/trafficdata --count 0
```

![]({{ site.baseurl }}/images/awsbigdata/rpilogs.png)

**AWS IOT CORE**

1. Create AWS IOT Rule.
2. Create Action and Select Send a message to an Amazon Kinesis Stream.
3. Create AWS Kinesis Stream add stream to the action.
4. Create Error Action for records in error.
5. Enable the Rule.

![]({{ site.baseurl }}/images/awsbigdata/iot-rule-kinesis.png)

Verify Data Getting Published to Kinesis Stream using Kinesis Analytics

**AWS EMR**

Spark job reads and processes the kinesis stream data as DStreams.

Provide DynamoDB table as checkpoint location in checkpointAppName parameter

```scala
// Kinesis DStreams  create 1 Kinesis Receiver/input DStream for each shard
 val kinesisStreams = (0 until numStreams).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .streamName(streamName)
        .checkpointAppName(appName)
        .initialPosition(new Latest())
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
  }
```

Method to Get Vehicle owner data from Redshift based on License Plate

```scala
def getDriverData(licenseplate:String) = {
    val url = "jdb:url"
    val user = "awsuser"
    val redShifTable = "public.rtr_iot_driver_data"

    // Get owner data from a Redshift table
    val driverDf = spark.read
      .format("jdbc")
      .option("url",url)
      .option("driver","com.amazon.redshift.jdbc42.Driver")
      .option("user",user)
      .option("password","We12come")
      .option("dbtable",redShifTable )
      .load()
      .createOrReplaceTempView("driver_data")

    spark.sql( s"select first_name,last_name,licenseplate,myphone from driver_data where licenseplate = '$licenseplate'")

  }

```

Filter the Streaming DataSet for speeding vehicles above speed limit

```scala
val speedingVehicles = payLoadDs.filter(col("speed")>75)
```

1. Join the streaming data frame with owner data from Redshift
2. Publish Joined Data Frame to Kinesis Stream
3. Put Record of the data in json format after joining with Driver Data and Speeding Dataframe into second Kinesis Stream

```scala
licensePlateDF.foreach(plate => {
 println("Speeding plate",plate)
    
 val driverData = getDriverData(plate)
    
// join condition between redshift df and streaming df   
 val joinCondition = speedingVehicles.col("licensePlate") === driverData.col("licenseplate")
    
// join the data frames using joinCondition
 val speedingDriver = speedingVehicles.join(driverData, joinCondition, "inner").drop(driverData.col("licenseplate"))
 println("Speeding driver details after df join")  
 speedingDriver.show()
    
 val data = speedingDriver.toJSON.map(row => row.toString).collect()(0)
 println("Speedign Drive Data toJson ",data)
 val kinesisOutStream = "rtr-iot-kinesis-out-stream"

 // Put the record onto the kinesis stream
 val putRecordRequest = new PutRecordRequest()
 putRecordRequest.setStreamName(kinesisOutStream)
 putRecordRequest.setPartitionKey(String.valueOf(UUID.randomUUID()))
 putRecordRequest.setData(ByteBuffer.wrap(data.getBytes()))
 kinesisClient.putRecord(putRecordRequest)
        })
```

Submit the Spark job from EMR Cluster

```shell
spark-submit iotkinesispark-assembly-0.1.jar IotKinesisCheckpoint  rtr-iot-kinesis-stream IotFastCarSensor https://kinesis.us-east-2.amazonaws.com
```

EMR Logs

![]({{ site.baseurl }}/images/awsbigdata/aws-emr-logs-small.png)

Verify Joined Data from Redshift and IOT Stream using Kinesis Analytics.

![]({{ site.baseurl }}/images/awsbigdata/spark-redshift.png)

**AWS SNS**

Create Topic and Subscription in AWS SNS

![]({{ site.baseurl }}/images/awsbigdata/sns-topic-subscription-sms.png)



**AWS Lambda**

Create AWS Lambda Function to Read from Kinesis Stream and Publish to SNS Topic.

![]({{ site.baseurl }}/images/awsbigdata/lambda-kinesis-sns.png)

AWS Lambda Python Code Reads Stream Data and Publishes to SNS Topic,with Phone number from data stream passed as Endpoint argument.

```python
phoneNumber = parsedPayload["myphone"]
e164phoneNumber = '+1'+phoneNumber
     
message_sns = "To {} {}, \n Your vehicle with licenseplate {} was clocked with speed above the permissible limit at {},{}\n Clocked speed {} \n Geo location {}, {} \n Time {}.".format(first_name,last_name,licensePlate,city,state,speed,longitude,latitude,iottimestamp)

logger.info(message_sns)

try:
  client.subscribe(TopicArn=topic_arn,Protocol='sms', Endpoint=e164phoneNumber)
  client.publish(TopicArn=topic_arn,Message=message_sns )
  logger.info('Successfully delivered Speeding alarm message to {} '.format(phoneNumber))
except Exception:
  logger.info('SNS Message Delivery failure')
```

**End Result** SMS Message Received from AWS SNS on the phone number of the owner of speeding vehicle.

![]({{ site.baseurl }}/images/awsbigdata/snsmessage.png)

**DynamoDb Streams Data Flow**

1. The Speeding vehicle data will be also written to DynamoDb by AWS EMR Spark job.
2. Enable streaming on DynamoDB table and use lambda to read the data.
3. AWS lambda will put the streaming records to a Kinesis FireHose Stream which will will save it to S3 destination.

Amazon EMR Spark script to write the Data to DynamoDb.

```scala
// write to dynamodb
speedingVehiclesDF.write
       .format("com.audienceproject.spark.dynamodb.datasource")
       .option("tableName", dynamoDbName)
       .option("region",regionName)
       .save()

```

![]({{ site.baseurl }}/images/awsbigdata/dynamodbresults.png)

1. Enable Streams on the DynamoDb Table.
2. Create Lambda Function which reads from the DynamoDb Stream and Publish to Kinesis Firehose.
3. Kinesis FireHose saves the data into S3 bucket in json.



![]({{ site.baseurl }}/images/awsbigdata/dynamodb-stream-lamdba.png)

```python
record['dynamodb']
# logger.info('DDB Record: ' + json.dumps(ddbRecord))
parsedPayload = ddbRecord['NewImage']

logger.info('Firehose Record: ' + firehoseRecord)

firehose.put_record(DeliveryStreamName=deliveryStreamName, Record={ 'Data': firehoseRecord}) 
```

Verify the Kinesis Firehose Data Put records using Kinesis Analytics

![]({{ site.baseurl }}/images/awsbigdata/dynamodb-stream-kinesis-analytics.png)

**References**

- **Refer the git for data and code**

- https://github.com/rupeshtr78/awsiot.git
- Data Generated Using Python Faker package.
- Recommended Courses 
  - [AWS Certified Data Analytics Specialty 2020 - Hands On!](https://www.udemy.com/course/aws-big-data/)
  - [Stephane Maarek ](https://www.udemy.com/user/stephane-maarek/)
- [Frank Kane](https://www.udemy.com/user/frank-kane-2/)
  - Scala Spark
  - https://rockthejvm.com



