package com.fanx.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}

object StructuredStreamingWithKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[2]")
      .appName("StructuredStreamingWithKafka")
      .getOrCreate()
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(queryStarted: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("Query started: "+queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("Query terminated: "+queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: StreamingQueryListener.QueryProgressEvent): Unit = {
        println("Query made progress: "+queryProgress.progress)
      }
    })

    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaUtil.BROKER_LIST)
      .option("subscribe", KafkaUtil.TOPIC)
      .load()

    val ds = df.selectExpr("CAST(value AS STRING)").as[String]
    val words = ds.flatMap(_.split(" ")).groupBy("value").count()

    val query = words.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .start()


    query.awaitTermination()
  }
}
