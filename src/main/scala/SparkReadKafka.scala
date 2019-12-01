import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Fucai
  * @date 2019/10/11
  */

object SparkReadKafka {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
                        .appName("kafka test")
                        .master("local[2]")
                        .getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc,Seconds(10))
    val kafkaParams = Map[String,String] (
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "group.id" -> "spark_streaming"
    )
    val lineMap = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("spark_log"))
    lineMap.foreachRDD(unit => unit.foreach(u => println(u)))

    ssc.start()
    ssc.awaitTermination()

  }

}
