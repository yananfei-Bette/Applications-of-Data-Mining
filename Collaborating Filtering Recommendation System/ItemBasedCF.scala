import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ItemBasedCF {

  def PearsonCorrelation(u1: Array[Double], u2:Array[Double]): Double = {
    val u1Mean = u1.sum / u1.length
    val u2Mean = u2.sum / u2.length
    val numer = u1.zipWithIndex.aggregate(0.0) (
      { case (acc, (rate, index)) =>
        acc + (rate - u1Mean) * (u2(index) - u2Mean)
      }, {
        (part1, part2) => part1 + part2
      }
    )
    val deno1 = math.sqrt(
      u1.aggregate(0.0) (
        {(acc, rate) => acc + (rate - u1Mean) * (rate - u1Mean)},
        {(part1, part2) => part1 + part2}
      )
    )
    val deno2 = math.sqrt(
      u2.aggregate(0.0) (
        {(acc, rate) => acc + (rate - u2Mean) * (rate - u2Mean)},
        {(part1, part2) => part1 + part2}
      )
    )
    // return
    math.abs(numer / (deno1 * deno2))
  }

  def main(args: Array[String]): Unit = {
    // Start time recording
    val start = System.nanoTime()
    // Create Spark Configure
    val sparkConf = new SparkConf()
      .setAppName("ItemBasedCF")
      .setMaster("local")
      .set("spark.driver.host", "localhost")
    // Create Spark Context
    val sparkContext = new SparkContext(sparkConf)

    println("**************** ItemBased CF ************************")

    // Read training data
    val trainingDataFile = sparkContext.textFile(args(0))
    val trainingHeader = trainingDataFile.first()
    val trainingData = trainingDataFile.filter(line => line != trainingHeader)
      .map(line => line.split(","))
      .map(line => (line(0).hashCode(), line(1).hashCode(), line(2).toDouble))
    // trainingData.take(10).foreach(println)
    // (-446670649,1118817623,3.0)

    // Read testing data
    val testingDataFile = sparkContext.textFile(args(1))
    val testingHeader = testingDataFile.first()
    val testingData_ = testingDataFile.filter(line => line != testingHeader)
      .map(line => line.split(","))
      .map(line => (line(0), line(1), line(2)))

    val testingData = testingData_
      .map(line => (line._1.hashCode(), line._2.hashCode(), line._3.toDouble))
    // testingData.take(10).foreach(println)
    // (-446670649,1648533597,4.0)

    // Create string map for recovering id from hashcode
    val testingStringMap = testingData_
      .map(line => ((line._1.hashCode(), line._2.hashCode()), (line._1, line._2)))
      .collect()
      .toMap

    /////////////////////////////////////////////////////
    // Create hashmap which key: User_id, value: businesses
    val usersBusinesses = trainingData
      .map{ case (user, business, rate) => (user, (business, rate))}
      .groupByKey()
      .collectAsMap()

    // Create hashmap which key:Business_id, value:users
    val businessesUsers = trainingData
      .map{ case (user, business, rate) => (business, (user, rate))}
      .groupByKey()
      .collectAsMap()

    // Create product similarity pairs
    val businesses = trainingData.map(line => (line._1, line._2))
    val businessPair = businesses.join(businesses).map(line => line._2).distinct()
    // businessPair.take(10).foreach(println)

    ///////////////////////////////////////////////////////
    // Calculate similarity
    // Directly use training set
    val similarityThreshold = 0.01
    val similarity = businessPair.map { case (business1, business2) => {
      val usersRate1 = businessesUsers(business1).toArray
      val usersRate2 = businessesUsers(business2).toArray
      val users1 = usersRate1.map(line => line._1)
      val users2 = usersRate2.map(line => line._1)
      val commonUsers = users1.intersect(users2)
      val len = commonUsers.length
      var similarityW = Double.NaN
      if (len > 1) {
        val b1 = Array.fill[Double](len)(elem = 0)
        val b2 = Array.fill[Double](len)(elem = 0)
        var index1 = 0
        for ((business, rate) <- usersRate1.sortBy(line => line._1)) {
          if (commonUsers.contains(business)) {
            b1(index1) = rate
            index1 += 1
          }
        }
        var index2 = 0
        for ((business, rate) <- usersRate2.sortBy(line => line._1)) {
          if (commonUsers.contains(business)) {
            b2(index2) = rate
            index2 += 1
          }
        }
        similarityW = PearsonCorrelation(b1, b2)
      }
      ((business1, business2), similarityW)
    }
    }.filter(line => line._2 >= similarityThreshold)
      .collectAsMap()
    // similarity.take(10).foreach(println)

    ////////////////////////////////////////////////////////////
    // According to the similarity get predictions
    val predictions_ = testingData
      .filter(line => usersBusinesses.contains(line._1))
      .map{ case (targetUser, targetBusiness, rate) => {
        val businessesRate = usersBusinesses(targetUser)
        val k = 10
        val buffLen = 1
        var buff = List[(Double, Double)]()
        for ((business, rate) <- businessesRate) {
          if (similarity.contains(targetBusiness, business)
            && similarity(targetBusiness, business) != Double.NaN) {
            buff = (rate, similarity(targetBusiness, business))::buff
          }
          else if (similarity.contains(business, targetBusiness)
            && similarity(business, targetBusiness) != Double.NaN) {
            buff = (rate, similarity(business, targetBusiness))::buff
          }
        }

        var predict = 2.5
        val avgtemp = businessesRate.map(line => line._2)
        val avgRate = avgtemp.sum / avgtemp.size

        if (buff.length >= buffLen) {
          val temp = buff.sortBy(line => line._2).take(k)
            .map (line => {
              val numer = line._1 * line._2
              val deno = math.abs(line._2)
              (numer, deno)
            }).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
          predict = temp._1 / temp._2
        } else {
          predict = avgRate
        }
        (targetUser, targetBusiness, predict, avgRate)
      }
      }
    // predictions_.take(10).foreach(println)

    val predictions = predictions_
      .map{ case (targetUser, business, predictRate, _) =>
        ((targetUser, business), predictRate)}
      .collectAsMap()

    val targetUserAve = predictions_
      .map{ case (targetUser, _, _, avgRate) =>
        (targetUser, avgRate)}
      .distinct()
      .collectAsMap()

    //////////////////////////////////////////////////////////////
    // predict results
    val absDiff = testingData.map{ case (targetUser, business, rate) => {
      var predict_rate = 2.5
      if (predictions.contains((targetUser, business))) {
        predict_rate = predictions((targetUser, business))
      }
      else if (targetUserAve.contains(targetUser)) {
        predict_rate = targetUserAve(targetUser)
      }
      val err = predict_rate - rate
      math.abs(err)
    }
    }

    val pred = absDiff.map{ diff =>
      if (diff >= 0 && diff < 1) ">= 0 and < 1"
      else if (diff >= 1 && diff < 2) ">= 1 and < 2"
      else if (diff >= 2 && diff < 3) ">= 2 and < 3"
      else if (diff >= 3 && diff < 4) ">= 3 and < 4"
      else ">= 4"
    }.countByValue()
    val res = pred.toArray.sorted.map(line => {
      val pred_0 = line._1
      val pred_1 = line._2
      pred_0 + ": " + pred_1 + "\n"
    }).reduce(_ + _)
    println(res)

    val RMSE = math.sqrt(absDiff.map(err => err * err).mean())
    println(s"RMSE = $RMSE \n")

    val time = (System.nanoTime() - start) / 1e9d
    println(s"Time is $time seconds \n")

    // Save results
    val outputRes = testingData.map{
      case (targetUser, business, _) => {
        val strings = testingStringMap((targetUser, business))
        var predict_rate = 2.5
        if (predictions.contains((targetUser, business))) {
          predict_rate = predictions((targetUser, business))
        }
        else if (targetUserAve.contains(targetUser)) {
          predict_rate = targetUserAve(targetUser)
        }
        (strings._1, strings._2, predict_rate)
      }
    }.collect()
      .sortBy(line => (line._1, line._2))
      .map(line => {
        val col_0 = line._1
        val col_1 = line._2
        val col_2 = line._3
        col_0 + "," + col_1 + "," + col_2 + "\n"
      }).reduce(_ + _)

    new PrintWriter("Yanan_Fei_ItemBasedCF.txt") {
      write(outputRes)
      close()
    }

  }

}
