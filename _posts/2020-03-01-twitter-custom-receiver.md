---
layout: post
title: Twitter Custom Spark Receiver
tags: [spark,streaming,receiver,scala,jekyll]
image: '/images/twitter/twitter-login.png'
---

Create your twitter feed filtering the handles you follow and also filtering out retweets by those handles using custom Spark receiver. 

Create Twitter Developer Account from https://developer.twitter.com/apps

Get the credentials Create app then Keys and Tokens under app get the required Tokens.

Create twitter4j.properties file

```shell
debug=true
oauth.consumerKey=XXXSetOfqG154C2XXX
oauth.consumerSecret=XXXX
oauth.accessToken=XXXX
oauth.accessTokenSecret=XXXX
```

![](/images/twitter/twitter-dev-acc-png.png)



#### Create Twitter Receiver by filtering your the handles you follow 

```scala
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterFactory, TwitterStream, TwitterStreamFactory}

import scala.concurrent.Promise

class TwitterMyFeedReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY){

  import scala.concurrent.ExecutionContext.Implicits.global

  val twitterStreamPromise = Promise[TwitterStream]
  val twitterStreamFuture = twitterStreamPromise.future

  private def simpleStatusListener = new StatusListener {

    override def onStatus(status: Status): Unit = store(status)
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()
    override def onStallWarning(warning: StallWarning): Unit = ()
    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

  override def onStart(): Unit = {

    val twitterStream = new TwitterStreamFactory("src/main/resources")
      .getInstance()
      .addListener(simpleStatusListener)
      .sample("en")

      // Filter by your Friends
    val screenName = "XXXXXX"
    val twitter = new TwitterFactory("src/main/resources").getInstance()
    val twitterFriends = twitter.getFriendsIDs(screenName, -1)  
    val myFriendsId = twitterFriends.getIDs
    val myFilterQuery = new FilterQuery()

    val myTwitterStream = {
      twitterStream.filter(myFilterQuery.follow(myFriendsId: _*))
    }

    twitterStreamPromise.success(myTwitterStream)
  }

  override def onStop(): Unit = twitterStreamFuture.foreach{ twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }

}

```

#### Create Twitter Feed Filtering out Retweets

```scala
package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

import twitter4j.Friendship

object TwitterMyFeed {


  val spark = SparkSession.builder()
    .appName("TwitterProject")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))


  def readTwitter():Unit = {
    val twitterStream : DStream[Status] = ssc.receiverStream(new TwitterMyFeedReceiver)
    val tweets = twitterStream.map { status =>
      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val userScreenName = status.getUser.getScreenName
      val retweet = status.isRetweet
      val text = status.getText

      s"User: $username UserScreenName: $userScreenName ($followers followers) retweet: $retweet tweets: $text"

    }

    tweets.print()
    ssc.start()
    ssc.awaitTermination()

  }

  def readTwitterFiltered():Unit = {
    val twitterStream  = ssc.receiverStream(new TwitterMyFeedReceiver)
    .map { status => (
      status.getUser.getName,
      status.getId,
      status.getUser.getId,
      status.getUser.getFollowersCount,
      status.getUser.getScreenName,
      status.isRetweet,
      status.isFavorited,
      status.getFavoriteCount,

      status.getText )
      }

    // Retweet Filter
    twitterStream.filter(tweet =>
      tweet._7.equals(false)
    ).print()

  }

  def main(args: Array[String]): Unit = {

    readTwitterFiltered()
    ssc.start()
    ssc.awaitTermination()

  }

}
```

Results

![](/images/twitter/myfeedresult.png)