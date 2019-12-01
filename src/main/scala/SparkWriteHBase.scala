import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapred.{TableOutputFormat => MapredTableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Fucai
  * @date 2019/9/20
  */

object SparkWriteHBase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("spark write hbase").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val hbaseConf = HBaseConfiguration.create()
    // 设置zookeeper地址
    hbaseConf.set("hbase.zookeeper.quorum","master,slave1,slave2")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"student")

    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[MapredTableOutputFormat])

    val indataRdd = sc.makeRDD(Array("5,fucai,F,5"))
    val rdd = indataRdd.map(_.split(","))
                        .map( arr=> {
                          //行键值
                          val put = new Put(Bytes.toBytes(arr(0)))
                          put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
                          put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))
                          put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3)))
                          (new ImmutableBytesWritable(),put)
                        })
    rdd.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
}
