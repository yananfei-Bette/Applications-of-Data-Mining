import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.util.Random._
// import scala.util.control.Breaks._

import java.io.PrintWriter

object Task1 {

  /////////////////////// word Count ///////////////////////////
  def wordCount(Data: Iterator[List[String]]): Iterator[List[(String, Double)]] = {
    // Data.foreach(println)
    val res = Data.map(x => {
      x.groupBy((word: String) => word)
        .mapValues(_.length.toDouble)
        .toList
        .sortWith(_._1 < _._1)

    })
    // res.take(10).foreach(println)
    res
  }

  ////////////////////// TF-IDF ///////////////////////////////
  def tf_norm(Data: Iterator[List[String]]): Iterator[List[(String, Double)]] = {
    // CALCULATE TF SCORES
    // REFERENCE: https://janav.wordpress.com/2013/10/27/tf-idf-and-cosine-similarity/
    // println("************* Data ***********")
    // println(Data.size)

    /* shift */
    ////////// check!!!
    ///////// Done
    val tf = Data.map(x => {
      val review = x.groupBy((word: String) => word)
        .mapValues(_.length)
        .toList
        .sortWith(_._1 < _._1)
      val sum = review.map(x => x._2).sum
      (review, sum)
    })
    // println("*********** tf ************")
    // tf.take(10).foreach(println)
    // println("************* tf *************")
    // println(tf.size)


    val tf_norm = tf.map(x => {
      x._1.map(word => (word._1, word._2.toDouble / x._2))
    })
    // println("*********** tf_norm *********")
    // tf_norm.take(10).foreach(println)
    // println("*************** tf_norm *************")
    // println(tf_norm.size)
    tf_norm
  }

  ///////////////////// Distance /////////////////////////////
  def Distance(v1: Array[Double], v2: Array[Double]): Double = {
    val res = v1.zip(v2)
      .map(x => math.pow(x._1 - x._2, 2))
      .sum
    math.sqrt(res)
  }

  def cosSimilarity(v1: Array[Double], v2: Array[Double]): Double = {
    // calculate Similarities
    val numer = v1.zip(v2).map(x => x._1 * x._2).sum
    val deno_1 = math.sqrt(v1.zip(v1).map(x => x._1 * x._2).sum)
    val deno_2 = math.sqrt(v2.zip(v2).map(x => x._1 * x._2).sum)

    numer.toDouble / (deno_1 * deno_2)
  }



  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()

    ///////////////////// Creat spark context /////////////
    val sparkConf = new SparkConf()
      .setAppName("Yanan_Fei_Task1")
      .setMaster("local")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(sparkConf)

    ////////////////////// read data //////////////////////
    val DataPath = args(0)
    val DataFile = sc.textFile(DataPath)
    val Data = DataFile.map(x => x.split(" ").toList)
    // Data.take(10).foreach(println)
    println(s"Data is loading......$DataPath \n")

    // mark List
    // val DataList = Data.collect.toList.zipWithIndex

    val feature = args(1).toUpperCase()
    assert(feature == "W" || feature == "T")
    println(s"Feature type: $feature \n")

    val N = args(2).toInt
    println(s"Number of cluster: $N \n")

    val iterations = args(3).toInt
    println(s"Max number of iterations: $iterations \n")

    /////////////////////// feature processing ///////////////////
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

    ////////// word count /////////////
    if (feature == "W") {
      samples = Data.mapPartitions(chunk => {
        val res = wordCount(chunk)
        res
      }
      ).collect()
      // samples.take(10).foreach(println)
      // println("**************")
      // println(samples.length)

      vectors = samples.map(x => {
        val vector = Array.fill[Double](wordsDicLen)(elem = 0.0)
        for ((word, count) <- x) {
          vector(wordsDic(word)) = count
        }
        // println(vector.toList)
        vector
      }).toList
      // vectors.take(10).foreach(println)

      numVectors = vectors.length
      // println(numVectors)
      // 1000

      //////////// TF-IDF //////////
      // CALCULATE TF-IDF SCORES
    } else {
      // tf_norm
      samples = Data.mapPartitions(chunk => {
        //println("********** chunk **********")
        //println(chunk.size)
        val res = tf_norm(chunk)
        // println("********** res ************")
        // println(res.length)
        res
      }
      ).collect()
      // sample.take(10).foreach(println)
      // println("*********** total ********")
      // println(samples.length)

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
    }

    ///////////////////// k-means ////////////////////////////
    setSeed(20181031)
    // setSeed(1000)
    val indexes = shuffle(List.range(0, numVectors)).slice(0, N)
    // println(indexes.length)
    // println(indexes)
    // List(605, 674, 373, 940, 918)

    var centroids = indexes.map(x => vectors(x))
    // List
    // println("*****************")
    // centroids.foreach(println)
    // array[Double]
    // println(centroids(N - 1).toList.sum)

    assert(centroids.length == N)

    var rFinal = Array.ofDim[Int](numVectors, N)
    // assert(rFinal(numVectors - 1)(N - 1) == 0)
    var wsseFinal = 0.0
    var sseFinal = Array.fill[Double](N)(0.0)

    // Repeat
    for (iter <- 0 until iterations) {
      println(s"******************** iter: $iter *********************")
      // Initial a distance matrix
      val r_distance = Array.ofDim[Double](numVectors, N)
      for (i <- 0 until numVectors) {
        for (k <- 0 until N) {
          r_distance(i)(k) = Distance(vectors(i), centroids(k))
          // r_distance(i)(k) = cosSimilarity(vectors(i), centroids(k))
          // println(r_distance(i)(k))
        }
        // println(r_distance(i).toList)
      }

      // find the closest centroid
      val r = Array.ofDim[Int](numVectors, N)
      for (i <- 0 until numVectors) {
        val rIndex_min = r_distance(i)
          .zipWithIndex
          .minBy(_._1)._2

        // val rIndex_max = r_distance(i)
        //  .zipWithIndex
        //  .maxBy(_._1)._2
        // println(r_distance(i).toList)
        // println(rIndex_min)
        r(i)(rIndex_min) = 1
        // r(i)(rIndex_max) = 1
      }

      // compute wsse
      //////// error
      val sse = Array.fill[Double](N)(0.0)
      for (i <- 0 until numVectors) {
        for (k <- 0 until N) {
          if (r(i)(k) != 0){
            sse(k) += centroids(k)
              .zip(vectors(i))
              .map(x => math.pow(x._1 - x._2, 2)).sum
          }
        }
      }
      val wsse = sse.sum
      ///////////

      // update centroids
      val r_sum_sample = r.transpose.map(_.sum).toList
      println("=========== clustering ===========")
      r_sum_sample.foreach(println)
      println("*********** error ****************")
      sse.foreach(println)
      println(s"wsse: $wsse")
      println("==================================")
      /*1 1 961 33 4*/

      for (k <- 0 until N) {
        //println(k)
        if (r_sum_sample(k) != 0) {
          var sum_n = Array.fill[Double](wordsDicLen)(0.0)
          for (i <- 0 until numVectors) {
            if (r(i)(k) != 0) {
              val temp = vectors(i).map(_ * r(i)(k))
              ////!!!!!!
              sum_n = sum_n.zip(temp).map((x: (Double, Double)) => x._1 + x._2)
              // println(sum_n.toList)
            }
          }
          ////!!!!!!
          val oldCentroid = centroids(k)
          val newCentroid = sum_n.map(x => x / r_sum_sample(k))
          val diff = newCentroid.zip(oldCentroid).map(x => math.abs(x._1 - x._2)).sum
          // update
          centroids = centroids.updated(k, sum_n.map(x => x / r_sum_sample(k)))
          println(s"update centriods $k, and Diff is $diff")
        }
      }
      // update rFinal
      rFinal = r
      wsseFinal = wsse
      sseFinal = sse
      // break
    }

    // get Index for each samples
    val membershipIndexes = rFinal.map(x => x.zipWithIndex.maxBy(_._1)._2.toInt)
    // membershipIndexes.take(10).foreach(println)

    ///////////////////// output /////////////////////////
    val centroidsCount = membershipIndexes.map(x => (x, 1)).groupBy(_._1).map(x => (x._1, x._2.length)).toList.sortBy(_._1)
    println("============ final clustering ================")
    centroidsCount.foreach(println)
    println("==============================================")
    // println(membershipIndexes.length)

    // SAVE INTO JSON
    var res = s"""{\n    "algorithm": "K-Means", \n    "WSSE":$wsseFinal,\n    "clusters":[  """
    // println(res)

    for (k <- 0 until N) {
      val id = centroidsCount(k)._1
      val size = centroidsCount(k)._2
      val error = sseFinal(k)
      var wordsList = List[String]()
      // println("******************** test ***********************")
      for (i <- 0 until numVectors) {
        if(rFinal(i)(k) != 0) {
          val currVector = vectors(i)
          for (elem <- 0 until wordsDicLen) {
            if (currVector(elem) != 0) {
              // println("***************************************")
              wordsList = wordsList :+ wordsDicInverse(elem)
              // println(wordsList)
            }
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

    // println(res.dropRight(1) + s"""}""")

    val filename = args(1).toUpperCase() + "_" + args(2) + "_" + args(3)
    new PrintWriter("Yanan_Fei_KMeans_small_" + filename + ".json") {
      write(res.dropRight(1) + s""" ]\n}""")
      close()
    }

    ///////////////////// running time ////////////////////
    val time = (System.nanoTime() - startTime) / 1e9d
    println(s"Time is $time seconds \n")
  }

}
