import org.apache.spark.sql.SparkSession

object StructuredStreamingWordCount {
  def main(args: Array[String]): Unit = {
    //创建一个SparkSession对象
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("StructuredStreamingWordCount")
      .getOrCreate()
    //用来将RDD隐式地转换成DataFrame
    import spark.implicits._
    /*
    从socket连接中创建一个DataFrame
    lines代表一个流文本数据的无边界表,此表只包含一个列用来存放流入的数据,列名为value
     */
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    /*
    * 调用as[String]将DataFrame转换成Dataset<String>
    * 然后使用flatMap将每一行切分成多个单词,所得到的words变量中包含了所有的单词.
    * */
    val words = lines.as[String].flatMap(_.split(" "))
    /*通过分组来计数*/
    val wordCounts = words.groupBy("value").count()
    /*
    设置相应的流查询
    这里使用Complete模式,也就是每次更新时会使用当前批次完整的记录
     */
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}