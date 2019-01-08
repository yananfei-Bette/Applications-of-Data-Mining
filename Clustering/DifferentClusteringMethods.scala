import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, BisectingKMeans, GaussianMixture}
import org.apache.spark.mllib.linalg.Vectors

// import scala.util.Random._
// import scala.util.control.Breaks._

import java.io.PrintWriter

object Task2 {
  ////////////////////// TF-Norm ///////////////////////////////
  def tf_norm(Data: Iterator[List[String]]): Iterator[List[(String, Double)]] = {
    val tf = Data.map(x => {
      val review = x.groupBy((word: String) => word)
        .mapValues(_.length)
        .toList
        .sortWith(_._1 < _._1)
      val sum = review.map(x => x._2).sum
      (review, sum)
    })

    val tf_norm = tf.map(x => {
      x._1.map(word => (word._1, word._2.toDouble / x._2))
    })

    tf_norm
  }

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()

    ///////////////////// Creat spark context /////////////
    val sparkConf = new SparkConf()
      .setAppName("Yanan_Fei_Task2")
      .setMaster("local")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(sparkConf)

    ////////////////////// read data //////////////////////
    val DataPath = args(0)
    val DataFile = sc.textFile(DataPath)
    val Data = DataFile.map(x => x.split(" ").toList)
    // Data.take(10).foreach(println)
    println(s"Data is loading......$DataPath \n")

    val algorithm = args(1).toUpperCase()
    assert(algorithm == "K" || algorithm == "B" || algorithm == "G")
    println(s"Feature type: $algorithm \n")

    val N = args(2).toInt
    println(s"Number of cluster: $N \n")

    val iterations = args(3).toInt
    println(s"Max number of iterations: $iterations \n")

    ////////////////////////////////////////////////////////
    // create words dic
    val Data_ = Data.flatMap(x => x)
      .distinct()
      .collect()
      .sorted
      .zipWithIndex
    // wordsDic.take(10).foreach(println)

    val wordsDic = Data_.collect{ case (word, index) => (word, index)}.toMap
    val wordsDicInverse = Data_.collect{ case (word, index) => (index, word)}.toMap

    val wordsDicLen = wordsDic.size
    // println(wordsDicLen)
    // 14948

    // init
    var samples = Array[List[(String, Double)]]()
    var vectors = List[Array[Double]]()
    var numVectors = 0

    // tf_norm
    samples = Data.mapPartitions(chunk => {
      val res = tf_norm(chunk)
      res
    }
    ).collect()

    // idf
    val reviewListDic = Data.map(x => {
      x.groupBy((word: String) => word)
        .mapValues(_.length)
        .toList
        .toMap
    }).collect.toList
    // reviewDic.take(10).foreach(println)

    val numReview = reviewListDic.length
    // println(numReview)
    // 1000

    val wordsIDF = wordsDic.map(x => {
      var count = 0
      for (i <- 0 until numReview) {
        if (reviewListDic(i).contains(x._1)) {
          count += 1
        }
      }
      (x._1, 1.0 + math.log(numReview.toDouble / count))
    })
    // wordsIDF.take(10).foreach(println)
    // println(wordsIDF.size)
    // 14948

    vectors = samples.map(x => {
      val vector = Array.fill[Double](wordsDicLen)(elem = 0.0)
      for ((word, tfNorm) <- x) {
        vector(wordsDic(word)) = tfNorm * wordsIDF(word)
      }
      // println(vector.toList)
      vector
    }).toList
    // vectors.take(10).foreach(println)

    numVectors = vectors.length
    // println(numVectors)
    // 1000


    ////////////////////////////////////////////////////////
    // Load and parse the data
    val distVectors = sc.parallelize(vectors)
    val parsedData = distVectors.map(v => Vectors.dense(v)).cache()

    if (algorithm == "K") {
      /////////////////////// kmeans ///////////////////////
      // Cluster the data using KMeans
      val kmeans = new KMeans().setInitializationMode("random")
        .setK(N)
        .setMaxIterations(iterations)
        .setSeed(42)
        .run(parsedData)

      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = kmeans.computeCost(parsedData)
      println("**************** K-means *******************")
      println("Within Set Sum of Squared Errors = " + WSSSE)

      val clusters = kmeans.clusterCenters

      val predicts = parsedData.map(x => {
        val id = kmeans.predict(x)
        (id, x)
      }).groupByKey()

      // val test = predicts.take(1).map(x => x._2.toList).map(x => x.map(elem => elem.size))
      // test.foreach(println)
      // List(14948)

      val predictsDic = predicts.map(x => (x._1, x._2.toList)).collectAsMap

      val predictsCount = predicts.map(x => (x._1, x._2.size)).collectAsMap
      // predictsCount.foreach(println)

      var res = s"""{\n    "algorithm": "K-Means", \n    "WSSE":$WSSSE,\n    "clusters":[  """

      for (k <- 0 until N) {
        val id = k
        val size = predictsCount(k)
        val centriod = clusters(k)
        val belongings = predictsDic(k)

        var error = 0.0
        var wordsList = List[String]()

        error = belongings.map(x => x.toArray.zip(centriod.toArray)
          .map(elem => math.pow(elem._1 - elem._2, 2)).sum).sum
        // error = kmeans.computeCost(sc.parallelize(belongings))

        for (i <- 0 until size) {
          val currVector = belongings(i)
          for (elem <- 0 until wordsDicLen) {
            if (currVector(elem) != 0) {
              wordsList = wordsList :+ wordsDicInverse(elem)
            }
          }
        }

        val termsTotal = wordsList.groupBy((word: String) => word)
          .mapValues(_.length.toDouble)
          .toList
          .sortWith(_._2 > _._2)

        var termsSize = termsTotal.length

        if (termsSize >= 10) {
          termsSize = 10
        }

        val terms = termsTotal.take(termsSize).map(x => x._1).map(x => "\"" + x + "\"" + ",").reduce(_ + _).dropRight(1)

        res += s"""{\n        "id": $id, \n        "size": $size, \n        "error": $error, \n        "terms": [$terms]\n    },"""

      }

      ///////////////////////// Save //////////////////////////////
      val filename = args(1).toUpperCase() + "_" + args(2) + "_" + args(3)
      new PrintWriter("Yanan_Fei_Cluster_small_" + filename + ".json") {
        write(res.dropRight(1) + s""" ]\n}""")
        close()
      }

    } else if (algorithm == "B") {
      // using bisecting k-means
      val bkm = new BisectingKMeans()
        .setK(N)
        .setMaxIterations(iterations)
        .setSeed(42)
        .run(parsedData)

      val WSSE = bkm.computeCost(parsedData)
      println("*************** Bisecting K-Means *******************")
      println("Within Set Sum of Squared Errors = " + WSSE)

      // val clusters = bkm.clusterCenters

      val predicts = parsedData.map(x => {
        val id = bkm.predict(x)
        (id, x)
      }).groupByKey()


      val predictsDic = predicts.map(x => (x._1, x._2.toList)).collectAsMap

      val predictsCount = predicts.map(x => (x._1, x._2.size)).collectAsMap
      // predictsCount.foreach(println)

      var res = s"""{\n    "algorithm": "Bisecting K-Means", \n    "WSSE":$WSSE,\n    "clusters":[  """

      for (k <- 0 until N) {
        val id = k
        val size = predictsCount(k)
        // val centriod = clusters(k)
        val belongings = predictsDic(k)

        var error = 0.0
        var wordsList = List[String]()

        /*
        error = belongings.map(x => x.toArray.zip(centriod.toArray)
          .map(elem => math.pow(elem._1 - elem._2, 2)).sum).sum
          */
        error = bkm.computeCost(sc.parallelize(belongings))

        for (i <- 0 until size) {
          val currVector = belongings(i)
          for (elem <- 0 until wordsDicLen) {
            if (currVector(elem) != 0) {
              wordsList = wordsList :+ wordsDicInverse(elem)
            }
          }
        }

        val termsTotal = wordsList.groupBy((word: String) => word)
          .mapValues(_.length.toDouble)
          .toList
          .sortWith(_._2 > _._2)

        var termsSize = termsTotal.length

        if (termsSize >= 10) {
          termsSize = 10
        }

        val terms = termsTotal.take(termsSize).map(x => x._1).map(x => "\"" + x + "\"" + ",").reduce(_ + _).dropRight(1)

        res += s"""{\n        "id": $id, \n        "size": $size, \n        "error": $error, \n        "terms": [$terms]\n    },"""

      }

      ///////////////////////// Save //////////////////////////////
      val filename = args(1).toUpperCase() + "_" + args(2) + "_" + args(3)
      new PrintWriter("Yanan_Fei_Cluster_small_" + filename + ".json") {
        write(res.dropRight(1) + s""" ]\n}""")
        close()
      }

    } else {
      // Gaussian Mixture Model
      println("*************** Gaussian Mixture Model *******************")
      println("********** Memory issues in GMM on local host ************")
      println("**********************************************************")

      assert(args.length == 5, s"GMM is not suitable on local host. If you want to run this part, pls add an extra arg with Y (yes).")
      assert(args(4).toUpperCase == "Y")

      val gmm = new GaussianMixture()
        .setK(N)
        .setMaxIterations(iterations)
        .setSeed(42)
        .run(parsedData)

      // println("*************** Gaussian Mixture Model *******************")

      var res = s"""{\n    "algorithm": "GMM", \n    "clusters":[  """

      for (i <- 0 until gmm.k) {
        val id = i
        val weight = gmm.weights(i)
        val mean = gmm.gaussians(i).mu
        val std = gmm.gaussians(i).sigma

        res += s"""{\n        "id": $id, \n        "weight": $weight, \n        "mean": $mean, \n        "std": $std\n    },"""

      }

      ///////////////////////// Save //////////////////////////////
      val filename = args(1).toUpperCase() + "_" + args(2) + "_" + args(3)
      new PrintWriter("Yanan_Fei_Cluster_small_" + filename + ".json") {
        write(res.dropRight(1) + s""" ]\n}""")
        close()
      }

    }

    ///////////////////// running time ////////////////////
    val time = (System.nanoTime() - startTime) / 1e9d
    println(s"Time is $time seconds \n")

  }

}
