import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark 查询hbase
 *
  * @author Fucai
  * @date 2019/9/19
  */

object SparkOperateHBase {

  def main(args: Array[String]): Unit = {

//    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")
//    var sc = new SparkContext(sparkConf)

    val sparkSession = SparkSession.builder()
                        .appName("test")
                        .master("local[2]")
                        .getOrCreate()
    val sc = sparkSession.sparkContext

    val conf = HBaseConfiguration.create()
    // 设置zookeeper地址
    conf.set("hbase.zookeeper.quorum","master,slave1,slave2")
    conf.set("hbase.zookeeper.property.clientPort","2181")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE,"student")
    // 参数： table类型，key类型，value类型
    val stuRdd = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val count = stuRdd.count()
    stuRdd.cache()

    // 遍历输出
    stuRdd.foreach(x => x match {
      case (_, result) => {
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes(),"name".getBytes()))
        val gender = Bytes.toString(result.getValue("info".getBytes(),"gender".getBytes()))
        val age = Bytes.toString(result.getValue("info".getBytes(),"age".getBytes()))
        println("Row key:" + key + " Name:"+name+" gender:"+gender+ " age:"+ age)
      }
    })
  }
}
