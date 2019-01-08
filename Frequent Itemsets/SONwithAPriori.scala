import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.SortedSet

object Yanan_Fei_SON {

  // sort function
  def sort[A: Ordering] (coll: List[Iterable[A]]):List[Iterable[A]] = coll.sorted

  // generate next itemset
  def aPriori_gen(Lk_1: List[SortedSet[String]]): List[SortedSet[String]] = {
    var Ck = List[SortedSet[String]]()
    // for each l in Lk-1
    for ((l1, i) <- Lk_1.view.zipWithIndex) {
      for (l2 <- Lk_1.drop(i + 1)) {
        // Connect
        var canConnect = false
        if (l1.dropRight(1).equals(l2.dropRight(1)) && (l1.last < l2.last)) {
          canConnect = true
        }

        if (canConnect) {
          val c = l1 ++ l2

          // pruning
          var hasInfrequentSubset = true
          for (string <- c) {
            val s = c - string
            if (!Lk_1.contains(s)) {
              hasInfrequentSubset = true
            }
          }
          hasInfrequentSubset = false

          if (!hasInfrequentSubset) {
            Ck = c :: Ck
          }
        }
      }
    }
    // return Ck
    Ck
  }

  // apriori alg
  def aPriori(chunkList: List[Set[String]], threshold: Double): List[SortedSet[String]] = {
    var res = List[SortedSet[String]]()

    // l1
    val L1 = chunkList.flatten.groupBy(identity)
      .mapValues(x => x.size)
      .filter{case (_, value) => value >= threshold}
      .keys
      .map(x => SortedSet(x))
      .toList
    // L1.take(10).foreach(println)

    var Lk_1 = sort(L1).map(x => SortedSet[String]() ++ x.toSet)

    // from Lk-1 to Lk
    while (Lk_1.nonEmpty) {
      // save previous results
      res = res ::: Lk_1

      // generate candidates
      val Ck = aPriori_gen(Lk_1)

      // scan chunk and count
      val count = chunkList.flatMap(x => {
        var Ct = List[SortedSet[String]]()
        for (t <- Ck) {
          if (t.subsetOf(x)) {
            Ct = t :: Ct
          }
        }
        Ct
      }
      ).groupBy(identity)
        .mapValues(x => x.size)
        .filter{ case (_, value) => value >= threshold}
        .keys
        .toList

      // update Lk-1
      val Lk = sort(count)
          .map(x => SortedSet[String]() ++ x.toSet)
      Lk_1 = Lk
    }
    res
  }

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    // Create spark context
    val sparkConf = new SparkConf()
      .setAppName("Yanan_Fei_SON")
      .setMaster("local")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(sparkConf)

    //////////////////////////// alg starts ///////////////////////////////
    println("***************** SON *****************")
    // read data
    val DataFile = sc.textFile(args(0))
    val Data = DataFile
      .map(x => x.split(","))
      .map(x => (x(0), x(1)))
    // Data.take(10).foreach(println)
    /*
    (0,ive)
    (0,casino)
    (0,dont)
     */

    // set support
    val support = args(1).toDouble
    println(s"Support threshold: $support \n")
    // Support threshold: 30

    // baskets
    val baskets = Data.groupByKey().map(x => x._2.toSet)

    // num of partitions
    val numPartitions = baskets.getNumPartitions
    println(s"Number of Partitions: $numPartitions \n")
    // Number of Partitions: 1

    // reduce support threshold
    var threshold = 1.0
    if (threshold < support / numPartitions) {
      threshold = support / numPartitions
    }

    //////////////////////// Chunk ////////////////////////////
    val candidates = baskets.mapPartitions ( chunk => {
      val chunkList = chunk.toList
      val res = aPriori(chunkList, threshold)
      res.iterator
    }
    ).distinct()
      .collect()
    // candidates.take(10).foreach(println)

    /*
    val frequenItems = baskets.flatMap(x => {
      var temp = Seq(SortedSet("")).drop(1)
      for (c <- candidates) {
        if (c.toSet.subsetOf(x)) {

          temp = temp :+ c
        }
      }
      temp
    }
    ).countByValue()
      .filter {case (_, value) => value >= support}
      .keys
      */

    // improve speed
    val candidatesBroadcasted = sc.broadcast(candidates)

    // get results
    val results = baskets.mapPartitions(chunk => {
      val chunkList = chunk.toList
      var res = List[SortedSet[String]]()
      for (i <- chunkList) {
        for (j <- candidatesBroadcasted.value) {
          if (j.subsetOf(i)) {
            res = j :: res
          }
        }
      }
      res.iterator
    }
    ).countByValue()
      .filter(x => x._2 >= support)
      .keys
      .map(x => (x.size, x))

      /*
      .reduceByKey(_ + _)
      .filter(x => x._2 >= support)
      .map(x => x._1)
      .map(x => (x.size, x))
      .collect()
      */

    /*
    for (elem <- results.take(10)) {
      println(elem)
    }
    */

    /////////////////////////////////////////////////////////////
    // save
    /*
    val groupBySize = results.groupBy(x => x._1)
    // groupBySize.take(10).foreach(println)

    val maxSize = groupBySize.maxBy(x => x._1)._1.toInt

    var res = ""
    for (i <- 1.to(maxSize)) {
      val waitToSort = groupBySize.filter(x => x._1 == i).values
      println(waitToSort)
      // val sorted = sort(waitToSort)
    }
    */

    val groupBySize = results.groupBy(x => x._1)
      .mapValues( x => {
        val res = sort(x.map(y => y._2).toList)
        res
      })
    // groupBySize.take(10).foreach(println)

    var res = ""
    val maxSize = groupBySize.maxBy(x => x._1)._1
    for (i <- 1.to(maxSize)) {
      val eachI = groupBySize.filter(x => x._1 == i).values
      // println("***********************")
      // eachI.foreach(println)
      // res += eachI.map(x => "('" + x.mkString("', '") + "')").mkString(",") + "\n\n"
      for (set <- eachI) {
        // println("aaaaaaaaaaaaaa")
        // set.foreach(println)
        // res += "("
        for (elem <- set.toArray) {
          // println("bbbbbbbbbbbbbbbb")
          // elem.foreach(println)
          res += "("
          for (e <- elem) {
            // println("cccccccccccccc")
            // println(e)
            res = res + e + ","
          }
          // res = res + elem + ","
          //println("***************")
          //println(elem)
          //println(res)
          res = res.dropRight(1)
          res = res + "),"
        }
        // res.dropRight(1)
        // res = res + "),"
        // println("*****************")
        // println(res)
        res = res.dropRight(1)
        res += "\n\n"
      }
      // res.dropRight(1)
      // res += "\n\n"
      // println("******************")
      // println(res)
    }


    val filename = args(0).split("/").last.replace(".txt", "")
    // println(filename)

    new PrintWriter(args(2) + "/Yanan_Fei_SON_" + filename + "_" + args(1) + ".txt") {
      write(res)
      close()
    }

    /////////////////////////////////////////////////////////////
    // time
    // alg's time
    val time = (System.nanoTime() - startTime) / 1e9d
    println(s"Time is $time seconds \n")

  }
}
