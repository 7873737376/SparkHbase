import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object DataFrameToHBaseBatch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark HBase Batch Write - All Columns")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample DataFrame
    val df = Seq(
      ("row1", "value1", "valueA"),
      ("row2", "value2", "valueB"),
      ("row3", "value3", "valueC")
    ).toDF("rowkey", "col1", "col2")

    // HBase configuration
    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("your_table_name"))

    val columnFamily = "cf" // Change this to your actual column family

    // Convert DataFrame rows to HBase Put objects
    val puts = df.rdd.map(row => {
      val rowKey = row.getAs[String]("rowkey")
      val put = new Put(Bytes.toBytes(rowKey))

      row.schema.fields
        .filter(_.name != "rowkey")
        .foreach(field => {
          val colName = field.name
          val colValue = Option(row.getAs[Any](colName)).map(_.toString).getOrElse("")
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(colName), Bytes.toBytes(colValue))
        })

      put
    }).collect().toList.asJava

    // Perform batch put
    table.batch(puts, new java.util.ArrayList[Object]())

    // Cleanup
    table.close()
    connection.close()
    spark.stop()
  }
}


import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

def createHBasePut(row: Row, rowKeyCol: String, columnFamily: String): Put = {
  val rowKey = row.getAs[Any](rowKeyCol).toString
  val put = new Put(Bytes.toBytes(rowKey))

  row.schema.fields
    .filter(_.name != rowKeyCol)
    .foreach { field =>
      val colName = field.name
      val dataType = field.dataType
      val value = row.getAs[Any](colName)

      if (value != null) {
        val bytesValue = dataType match {
          case IntegerType => Bytes.toBytes(value.asInstanceOf[Int])
          case LongType    => Bytes.toBytes(value.asInstanceOf[Long])
          case DoubleType  => Bytes.toBytes(value.asInstanceOf[Double])
          case FloatType   => Bytes.toBytes(value.asInstanceOf[Float])
          case BooleanType => Bytes.toBytes(value.asInstanceOf[Boolean])
          case StringType  => Bytes.toBytes(value.toString)
          case _           => Bytes.toBytes(value.toString) // fallback
        }

        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(colName), bytesValue)
      }
    }

  put
}

How to Use It
val puts = df.rdd.map(row => createHBasePut(row, "rowkey", "cf")).collect().toList.asJava
You can now reuse createHBasePut across any DataFrame that has a row key column and consistent column family.

Let me know if you’d like to extend this to handle TimestampType, DecimalType, BinaryType, or nested structures.

