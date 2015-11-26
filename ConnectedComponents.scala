import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.Random

object ConnectedComponents {
  var stale = 0
  var max = 100000000

  def stringToTuple(vertices: String) : (String, String) = {
    val verticesArray = vertices.replaceAll("\\s+", " ").split(" ")
    return (verticesArray(0), verticesArray(1))
  }

  def rankValue(ranksMap: scala.collection.mutable.Map[String, Int], key: String) : Int = {
    if(ranksMap.contains(key)) {
      return ranksMap(key)
    }
    else {
      return max
    }
  }

  def main(args: Array[String]) {
    val randomRange = 1000000

    val conf = new SparkConf().setAppName("Simple PageRank")
    val sc = new SparkContext(conf)

    val lines = scala.io.Source.fromFile("/people/plgkalap/ar-pagerank/web-Google2").mkString
    val split_lines = lines.split("\\r?\\n");
    val linksData = split_lines.map( v => stringToTuple(v))

    val start = java.lang.System.currentTimeMillis();

    //val linksData = Array(("b", "a"), ("c", "a"), ("c", "d"), ("d", "a"), ("d", "b"))
    val linksRDD = sc.parallelize(linksData)

    val links = linksRDD.distinct().groupByKey().cache()
    val nodesNumber = links.count()

    val ranks = links.mapValues(v => Random.nextInt(randomRange)).collect()
    var ranksRDD = sc.parallelize(ranks)
    var ranksMap = scala.collection.mutable.Map.empty[String, Int]
    var iteration = 0

    while(stale < nodesNumber && iteration < 10) {
      iteration += 1
      stale = 0
      ranksRDD.collect.foreach { v => ranksMap(v._1) = v._2 }

      val contribs = links.join(ranksRDD).map {
        case (node, (nodes, rank)) =>
          val neighValues = nodes.map { v => rankValue(ranksMap, v) }
          node -> (neighValues ++ List(rank))
      }
      val sth = contribs.flatMap {
        case (node, list) =>
          list.map { ele =>
            (node ,ele)
          }
      }
      for (ele <- contribs) {
        println(ele._2.min + " == " + ranksMap(ele._1))
        if(ele._2.min == ranksMap(ele._1)) {
          println("STALE SHOULD INCREASE")
          stale += 1
        }
      }
      println("contribs.collect()(0) " + contribs.collect()(0))
      println("contribs.collect()(0)._2.min " + contribs.collect()(0)._2.min)
      println("ranksMap(contribs.collect()(0)._1) " + ranksMap(contribs.collect()(0)._1))
      println("contribs.collect()(0)._1 " + contribs.collect()(0)._1)
      ranksRDD = sth.reduceByKey(_ min _)
      println("ITEARTION " + iteration)
    }

    val output = ranksRDD.collect()
    val end = java.lang.System.currentTimeMillis();
    //output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    println("STALE " + stale)
    println("TIME IN MILI " + (end - start))
    sc.stop()
  }
}

