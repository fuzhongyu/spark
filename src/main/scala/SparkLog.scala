import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapred.{TableOutputFormat => MapredTableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  *
  * @author Fucai
  * @date 2019/11/25
  */

object SparkLog {

  val sparkSession = SparkSession.builder()
    .appName("kafka test")
    .master("local[2]")
    .getOrCreate()
  val sc = sparkSession.sparkContext


  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(sc,Seconds(10))
    val kafkaParams = Map[String,String] (
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "group.id" -> "spark_streaming"
    )
    val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("spark_log"))
    stream.foreachRDD(rdd => rdd.foreach(line => saveData2Hbase(line._2)))

    ssc.start()
    ssc.awaitTermination()
  }


  def saveData2Hbase(line:Any) = {

    val hbaseConf = HBaseConfiguration.create()
    // 设置zookeeper地址
    hbaseConf.set("hbase.zookeeper.quorum","master,slave1,slave2")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"user_action")

    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[MapredTableOutputFormat])

    val indataRdd = sc.makeRDD(Array(String.valueOf(line)))
    val rdd = indataRdd.map(_.split("\\t"))
      .map( arr=> {
        //行键值
        val put = new Put(Bytes.toBytes(arr(0)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("createTime"),Bytes.toBytes(arr(1)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("header"),Bytes.toBytes(arr(2)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("url"),Bytes.toBytes(arr(3)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("code"),Bytes.toBytes(arr(4)))
        (new ImmutableBytesWritable(),put)
      })
    rdd.saveAsHadoopDataset(jobConf)

  }


}
