---
layout: post
title: Spark PostGres Cassandra MongoDb
tags: [spark,postgres,cassandra,mongodb,jekyll]
image: '/images/jdbc/spark.png'
---

Integrate spark with Postgres, Cassandra,MongoDb

Explore how to write spark streaming data into databases.

You cannot write streams into these databases .We can write batches of data using *foreachbatch*

- MongoDB stores JSON documents in collections with dynamic schemas.MongoDb has Single-master.
- MongoDB Supports multiple indices Full-text indices for text searches .
- Cassandra, there is no master node– every node runs exactly the same software and performs the same functions.It’s non-relational, but has a limited CQL query language as its interface  
- PostgreSQL is a relational database management system.
- For analytic queries, Hive, Pig, Spark, etc. work great.
- For data at giant scale – export data to a non-relational database for fast and scalable serving of that data to applications like web.  

##### PostGres

```scala
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://192.168.1.200:5432/hyper"
  val user = "postgres"
  val password = "postgres"
  
  val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch{(batch : Dataset[Car] ,batchId: Long) =>
        batch.write
          .format("jdbc")
          .option("driver",driver)
          .option("url",url)
          .option("user",user)
          .option("password",password)
          .option("dbtable","public.cars")
          .save()
      }

      .start()
      .awaitTermination()
```

Verify data using db visualizer

![]({{ site.baseurl }}/images/jdbc/postgres-cars.png)



##### MongoDb

```scala
 val uri = "mongodb://192.168.1.200:27017/carsdataset.cars"

  def writetoMongoDb() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch{(batch : Dataset[Car] ,batchId: Long) =>
        batch.write
          .format("com.mongodb.spark.sql.DefaultSource")
          .option("uri",uri)
          .mode("append")
          .save()
      }

      .start()
      .awaitTermination()

  }

```

Verify Data Using MongoDb Compass

![]({{ site.baseurl }}/images/jdbc/mongo-compass-cars.png)



Read data from MongoDb Spark Sql

![]({{ site.baseurl }}/images/jdbc/mongo-spark-read.png)



##### Cassandra

```scala
    carsDS
      .writeStream
      .foreachBatch{(batch: Dataset[Car] , batchId: Long) =>
        batch.select(col("Name"),col("Horsepower"))
          .write
          .cassandraFormat("cars","hyper")
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()

  }
```

![]({{ site.baseurl }}/images/jdbc/cassandra-cqlsh.png)



##### Dependencies

```
  //mongodb
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1"
  
    // postgres
  "org.postgresql" % "postgresql" % postgresVersion,
  
    // cassandra 
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,
```

