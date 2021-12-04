package com.fanx.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import java.lang
import java.util.regex.Pattern
import scala.collection.JavaConversions


object StreamingWithKafka {
  private val checkpointDir = "/opt/app/kafka/checkpoint"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("StreamingWithKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointDir)


    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> KafkaUtil.BROKER_LIST,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> KafkaUtil.GROUP_ID,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
    )

//    createRDDDefiniteOffset(ssc, kafkaParams)

    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,
      subscribe1(kafkaParams))
    /*打印消费位移必须在混洗(shuffle)方法(也就对应下面的reduce方法)执行之前调用,
    因为一旦执行混洗方法后RDD与分区就不存在意义对应的映射关系了*/
    printOffsetsEachRDD(stream)

    val value = stream.map(record => {
      val intVal = Integer.valueOf(record.value())
      println(intVal)
      intVal
    }).reduce(_ + _)
    value.print()


    ssc.start
    ssc.awaitTermination
  }

  private def createRDDDefiniteOffset(ssc: StreamingContext, kafkaParams: Map[String, Object]) = {
    /*val offsetRanges = scala.Array(
      OffsetRange(topic, 0, 0.100),
      OffsetRange(topic, 1, 0.100),
      OffsetRange(topic, 2, 0.100),
      OffsetRange(topic, 3, 0.100)
    )
    val rdd = KafkaUtils.createRDD(ssc,
      JavaConversions.mapAsJavaMap(kafkaParams),
      offsetRanges, PreferConsistent)

    rdd.foreachPartition(records => {
      records.foreach(record => {
        println(record.topic() + ":" + record.partition() + ":" + record.value())
      })
    })*/
  }

  private def printOffsetsEachRDD(stream: InputDStream[ConsumerRecord[String, String]]) = {
    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    })
  }

  private val topic: String = KafkaUtil.TOPIC

  private def subscribe1(kafkaParams: Map[String, Object]) = {
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams)
  }

  private def subscribe2(kafkaParams: Map[String, Object]) = {
    ConsumerStrategies.SubscribePattern[String, String](
      Pattern.compile("topic-.*"), kafkaParams)
  }

  private def subscribe3(kafkaParams: Map[String, Object]) = {
    val partitions = List(new TopicPartition(topic, 0))
    ConsumerStrategies.Assign[String, String](partitions, kafkaParams)
  }

  private def subscribe4(kafkaParams: Map[String, Object]) = {
    val partitions = List(new TopicPartition(topic, 0))
    val fromOffsets = partitions.map(partition => {
      partition -> 5000L
    }).toMap
    ConsumerStrategies.Assign[String, String](partitions, kafkaParams, fromOffsets)
  }

  private def subscribe5(kafkaParams: Map[String, Object]) = {
    val partitions = List(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic, 2),
      new TopicPartition(topic, 3)
    )
    val fromOffsets = partitions.map(partition => {
      partition -> 5000L
    }).toMap
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams, fromOffsets)
  }


}
