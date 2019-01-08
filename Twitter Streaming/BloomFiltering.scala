import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.io.File
import scala.util.Random
import scala.util.hashing.{MurmurHash3 => MH3}

object BloomFiltering {

  // global variances
  val numHash = 6
  val width = 1000000
  val seed = 1

  // initialize
  val bits = Array.fill[Int](width)(0)
  val hashSeeds = Array.fill(numHash)(Random.nextInt(seed))

  // hashtages save
  var memory = Set[String]()

  // predict
  var correct = 0
  var incorrect = 0
  var total = 0
  var fp = 0
  var tn = 0

  def main(args: Array[String]): Unit = {
    /////////////// set spark config /////////////
    val sparkConf = new SparkConf()
      .setAppName("Yanan_Fei_Task2")
      .setMaster("local[2]")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(sparkConf)
    // Set the level of the log to OFF to eliminate the extra message
    sc.setLogLevel(logLevel = "OFF")

    ////////////// twitter key and token /////////
    val consumerKey = "RWHLtrmkWrrhGW1Ob891qKAsa"
    val consumerSecret = "CVjM4uq9IPqcS648AaaHvUtGQlJKM53citovV9wUofxesPGjZr"
    val accessToken = "828400071754407937-D1xLL3WRVrV1KzgqbSO58AcMTVJ9yXF"
    val accessTokenSecrect = "N8hVX0VpF7gQZrdlu4wAx5gNzZGTlSVPPV3f7lYf56Ge3"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecrect)

    val ssc = new StreamingContext(sc, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None)
    val hashTages = stream.flatMap(status =>
      status.getText.split(" ").filter(_.startsWith("#"))
    )//.window(Seconds(100))

    /////////////////////////////////////////////////
    // BF
    hashTages.foreachRDD(rdd => {
      for (item <- rdd) {
        total += 1
        var indexes = Array[Int]()
        var isExist = 1
        // check exist by BF
        for (i <- 0 until numHash) {
          // println("***************** test1 *****************")
          // https://stackoverflow.com/questions/33219638/how-to-make-a-hashcodeinteger-value-positive
          val ind = (MH3.stringHash(item, hashSeeds(i)) & 0xfffffff) % width
          // println("***************** test2 *****************")
          indexes = ind +: indexes
          // println("***************** test3 *****************")
          // println(ind)
          isExist &= bits(ind)
          // println("***************** test4 *****************")
        }
        if ((isExist == 1 && memory.contains(item))
          || (isExist != 1 && !memory.contains(item))) {
          if (isExist != 1 && !memory.contains(item)) {
            tn += 1
          }
          correct += 1
        } else {
          if (isExist == 1 && !memory.contains(item)){
            fp += 1
          }
          incorrect += 1
        }

        // add
        if (isExist != 1 && !memory.contains(item)){
          memory += item
          for (i <- 0 until numHash){
            bits(indexes(i)) = 1
          }
        }
      }
      val str1 = "***************** batches *****************************"
      val str2 = s"The number of the correct estimation: $correct \n"
      val str3 = s"The number of the incorrect estimation: $incorrect \n"
      var fpr = 0.0
      if (fp + tn != 0) {
        fpr = fp.toDouble / (fp + tn)
      }
      val str4 = s"The total number of hashTages: $total and the false positive rate: $fpr\n"

      println(str1)
      println(str2)
      println(str3)
      println(str4)

      // save
      File("log_BloomFiltering.txt").appendAll(str1 + "\n" + str2 + str3 + str4 + "\n\n\n")
    })

    /////////////////////////////////////////////////
    ssc.start()
    ssc.awaitTermination()
  }

}
