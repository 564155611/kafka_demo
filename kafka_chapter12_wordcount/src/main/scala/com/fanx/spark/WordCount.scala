package com.fanx.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/opt/app/spark/bin/spark-shell");
    val wordCount = rdd.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    val wordSort = wordCount.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    wordSort.saveAsTextFile("/tmp/spark")
    sc.stop()
  }
}
