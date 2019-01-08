// import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
// import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

object ModelBasedCF {

  def main(args: Array[String]): Unit = {
    // Create Spark Configure
    val sparkConf = new SparkConf()
      .setAppName("ModelBasedCF")
      .setMaster("local")
      .set("spark.driver.host", "localhost")
    //.set("spark.driver.bindAddress","127.0.0.1")
    // Create Spark Context
    val sparkContext = new SparkContext(sparkConf)

    println("**************** ModelBased CF ************************")

    // Read training data
    val trainingDataFile = sparkContext.textFile(args(0))
    val trainingHeader = trainingDataFile.first()
    // println(trainingHeader)
    // user_id,business_id,stars
    val trainingData = trainingDataFile.filter(line => line != trainingHeader)
      .map(line => line.split(","))
      .map(line => (line(0).hashCode(), line(1).hashCode(), line(2)))
      .map{ case (user, business, rate) =>
        Rating(user.toInt, business.toInt, rate.toDouble)
      }
    // trainingData.take(10).foreach(println)
    // (user_id,business_id,stars)
    // (YHWsLBS8jzZiPjKHMFOaAA,pgomg_u3H2RtEVUYUcngXQ,3)

    // Read testing data
    val testingDataFile = sparkContext.textFile(args(1))
    val testingHeader = testingDataFile.first()
    // println(testingHeader)
    // user_id,business_id,stars
    val testingData = testingDataFile.filter(line => line != testingHeader)
      .map(line => line.split(","))
      .map(line => (line(0).hashCode(), line(1).hashCode(), line(2)))
      .map{ case (user, business, rate) =>
        Rating(user.toInt, business.toInt, rate.toDouble)
      }
    // testingData.take(10).foreach(println)
    // (user_id,business_id,stars)
    // (q3AiAe-AcpDrNsdZf8nCvQ,7XTGtaKjANmZqp2QtN_iPA,4)

    //////////////////// training //////////////////////
    // Build the recommendation model using ALS
    val rank = 3
    val numIterations = 25
    val model = ALS.train(trainingData, rank, numIterations, 0.3, 1)

    /////////////////// testing ///////////////////////
    // Evaluate the model on testing data
    val usersBusiness = testingData.map{ case Rating(user, business, rate) =>
      (user, business)
    }
    val predictions =
      model.predict(usersBusiness).map{ case Rating(user, business, rate) =>
        ((user, business), rate)
      }
    val ratesAndPreds = testingData.map{ case Rating(user, business, rate) =>
      ((user, business), rate)}.join(predictions)

    /////////////////// evaluate //////////////////////
    val absDiff = ratesAndPreds.map { case ((user, business), (r1, r2)) =>
      val err = r1 - r2
      math.abs(err)
    }
    val pred = absDiff.map{ diff =>
      if (diff >= 0 && diff < 1) ">= 0 and <1"
      else if (diff >= 1 && diff < 2) ">= 1 and <2"
      else if (diff >= 2 && diff < 3) ">= 2 and <3"
      else if (diff >= 3 && diff < 4) ">= 3 and <4"
      else ">4"
    }.countByValue()
    val res = pred.toArray.sorted.map(line => {
      val pred_0 = line._1
      val pred_1 = line._2
      pred_0 + ": " + pred_1 + "\n"
    }).reduce(_ + _)
    println(res)

    val RMSE = math.sqrt(absDiff.map(err => err * err).mean())
    println(s"RMSE = $RMSE")

  }

}