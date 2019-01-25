import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random._
import scala.tools.nsc.io._

object TwitterStreaming {

  // global variance
  var memory = List[String]()
  var t = 0
  val memoryLength = 100
  var total = 0.0
  var average = 0.0

  setSeed(1)

  def main(args: Array[String]): Unit = {

    /////////////// set spark config /////////////
    val sparkConf = new SparkConf()
      .setAppName("Yanan_Fei_Task1")
      .setMaster("local[2]")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(sparkConf)

    ////////////// twitter key and token /////////
    val consumerKey = "XXXXXXXXXXXXXXX"
    val consumerSecret = "XXXXXXXXXXXXXXX"
    val accessToken = "XXXXXXXXXXXXXXX"
    val accessTokenSecrect = "XXXXXXXXXXXXXXX"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecrect)

    val ssc = new StreamingContext(sc, Seconds(15))
    val stream = TwitterUtils.createStream(ssc, None)

    val tweetsRdd = stream.map(status => status.getText)//.window(Seconds(150))

    tweetsRdd.foreachRDD(rdd => {
      for (item <- rdd) {
        t += 1
        if (memory.length < memoryLength) {
          memory = item :: memory

          total += item.length
          average = total / t

        } else {
          val randInt = nextInt(t)
          if (randInt < memoryLength){
            
            ///////////////// averge /////////////////////
            total += item.length - memory(randInt).length
            average = total / memoryLength

            memory = memory.updated(randInt, item)
            val hashTages = memory.flatMap(text => text.split(" ").filter(_.startsWith("#")))

            val sortHashTages = hashTages.map(hashTage => hashTage
              .replaceAll("\n", "")
              .replaceAll(",", "")
            )
              .groupBy((hashTage: String) => hashTage)
              .mapValues(_.length.toDouble)
              .toList
              .sortWith((x, y) => (x._2 > y._2) || (x._2 == y._2) && x._1 < y._1)

            var top5HashTages = ""
            if(sortHashTages.nonEmpty){
              top5HashTages = sortHashTages.take(5).map(_._1 + "\n").reduce(_ + _)
            }

            ////////////////////// print ////////////////////
            val numTweets = s"The number of the twitter from begining: $t \n"
            val top5 = s"Top 5 hot Hashtages:\n" + top5HashTages
            val averageLen = s"The average length of the twitter is: $average \n"
            println(numTweets)
            println(top5)
            println(averageLen)

            File("log_TwitterStreaming.txt").appendAll(numTweets + top5 + averageLen + "\n\n\n")

          }
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
