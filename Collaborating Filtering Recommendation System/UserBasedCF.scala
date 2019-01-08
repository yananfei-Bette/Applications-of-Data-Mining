import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object UserBasedCF {

  def PearsonCorrelation(u1: Array[Double], u2:Array[Double]): Double = {
    // println("***************")
    val u1Mean = u1.sum / u1.length
    val u2Mean = u2.sum / u2.length
    val numer = u1.zipWithIndex.aggregate(0.0) (
      { case (acc, (rate, index)) =>
        acc + (rate - u1Mean) * (u2(index) - u2Mean)
      }, {
        (part1, part2) => part1 + part2
      }
    )
    // println(numer)
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
    // println(numer, deno1, deno2)
    math.abs(
      numer / (deno1 * deno2)
    )
  }

  def main(args: Array[String]): Unit = {
    // Start time recording
    val start = System.nanoTime()
    // Create Spark Configure
    val sparkConf = new SparkConf()
      .setAppName("UserBasedCF")
      .setMaster("local")
      .set("spark.driver.host", "localhost")
      //.set("spark.driver.bindAddress","127.0.0.1")
    // Create Spark Context
    val sparkContext = new SparkContext(sparkConf)

    println("**************** UserBased CF ************************")

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

    val testingData = testingData_.map(line => (line._1.hashCode(), line._2.hashCode(), line._3.toDouble))
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

    // Create target user with its correlated users
    val targetUserwithCorrelatedUsers = testingData
      .filter(line => usersBusinesses.contains(line._1))
      .filter(line => businessesUsers.contains(line._2))
      .map{ case (targetUser, business, rate) => {
      val correlatedUsers = businessesUsers(business)
      (targetUser, correlatedUsers, business, rate)
      }
    }

    //Create target user and similar user pairs
    val targetUser_similarUser = targetUserwithCorrelatedUsers.flatMap{
      case (targetUser, correlatedUsers, _, _) => {
        val targetUser_value = usersBusinesses(targetUser)
        val line = correlatedUsers.map { case (user, _) => {
          /*
          if (targetUser == -446670649 && user == 37371038){
            println("11111111111111111")
          }
          */
          ((targetUser, user), targetUser_value.toArray, usersBusinesses(user).toArray)
        }
        }
        line
      }
    }.distinct()
    // targetUser_similarUser.take(10).foreach(println)

    //////////////////////////////////////////////////////////////
    // Calculate similarity
    val similarityThreshold = 0.3
    val similarity = targetUser_similarUser.map { case (userPair, targetUser_value, correlatedUser_value) => {
      val targetUser_business = targetUser_value.map(line => line._1)
      val correlatedUser_business = correlatedUser_value.map(line => line._1)
      val common_business = targetUser_business.intersect(correlatedUser_business)
      val len = common_business.length
      /*
      if (userPair == (-446670649,37371038)){
        println("222222222222222222")
      }
      */
      var similarityW = Double.NaN
      if (len > 1) {
        // initial to array to store common business
        val targetUser = Array.fill[Double](len)(elem = 0)
        val correlatedUser = Array.fill[Double](len)(elem = 0)
        // list targetUser's rates
        var targetIndex = 0
        for ((business, rate) <- targetUser_value.sortBy(line => line._1)) {
          if (common_business.contains(business)) {
            targetUser(targetIndex) = rate
            targetIndex += 1
          }
        }
        // list correlatedUser's rates
        var correlatedIndex = 0
        for ((business, rate) <- correlatedUser_value.sortBy(line => line._1)) {
          if (common_business.contains(business)) {
            correlatedUser(correlatedIndex) = rate
            correlatedIndex += 1
          }
        }
        similarityW = PearsonCorrelation(targetUser, correlatedUser)
      }
      (userPair, similarityW)
    }
    }.filter(line => line._2 >= similarityThreshold)
      .collectAsMap()
    // similarity.take(10).foreach(println)

    ////////////////////////////////////////////////////////////////
    // According to the similarity get predictions
    val k = 10
    val buffLen = 5
    val predictions_ = targetUserwithCorrelatedUsers.map {
      case (targetUser, correlatedUsers, business, _) => {
        // Initialize top-k similar Users
        var kSimilarUsers = List[(Int, Double)]()
        for ((user, _) <- correlatedUsers) {
          if (similarity.contains(targetUser, user) && similarity(targetUser, user) != Double.NaN){
            kSimilarUsers = (user, similarity(targetUser, user))::kSimilarUsers
          }
        }
        var variance = 0.0
        if (kSimilarUsers.length >= buffLen){
          val buff = kSimilarUsers.sortBy(line => line._2).take(k)
              .map{ case (user, similarityW) => {
                val sumRates = usersBusinesses(user).filter(line => line._1 != business)
                  .map(line => line._2).sum
                val meanRate = sumRates / (usersBusinesses(user).size - 1)
                // return
                val selectedUsers = usersBusinesses(user).filter(line => line._1 == business)
                val weightedRest = (selectedUsers.toArray.head._2 - meanRate) * similarityW
                (weightedRest, similarityW)
              }
              }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
          variance = buff._1 / buff._2
        }
        val targetSum = usersBusinesses(targetUser)
          .map(line => line._2).sum
        val targetMean = targetSum / usersBusinesses(targetUser).size
        // return
        (targetUser, business, targetMean + variance, targetMean)
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

    ////////////////////////////////////////////////////////////////////////
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

    new PrintWriter("Yanan_Fei_UserBasedCF.txt") {
      write(outputRes)
      close()
    }


  }
}
