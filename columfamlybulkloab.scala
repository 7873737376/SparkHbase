To handle multiple column families in your DataFrame-to-HBase batch write, you need a way to map each column to its corresponding column family.

Example Requirement:
You have:

Two column families: "cf1" and "cf2"
Two lists:
cf1Cols = List("col1", "col3")
cf2Cols = List("col2", "col4")
Then the logic should be:

Columns in cf1Cols go under column family "cf1"
Columns in cf2Cols go under column family "cf2"
Updated Code:
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object DataFrameToHBaseBatch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark HBase Batch Write - Multi CF")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample DataFrame
    val df = Seq(
      ("row1", "value1", "value2", "value3", "value4"),
      ("row2", "value5", "value6", "value7", "value8")
    ).toDF("rowkey", "col1", "col2", "col3", "col4")

    // Define column family mapping
    val cf1Cols = List("col1", "col3")
    val cf2Cols = List("col2", "col4")

    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("your_table_name"))

    val puts = df.rdd.map(row => {
      val rowKey = row.getAs[String]("rowkey")
      val put = new Put(Bytes.toBytes(rowKey))

      row.schema.fields.foreach(field => {
        val colName = field.name
        if (colName != "rowkey") {
          val colValue = Option(row.getAs[Any](colName)).map(_.toString).getOrElse("")
          val cf = if (cf1Cols.contains(colName)) "cf1"
                   else if (cf2Cols.contains(colName)) "cf2"
                   else "default_cf" // Optional fallback
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(colValue))
        }
      })

      put
    }).collect().toList.asJava

    // Batch write
    table.batch(puts, new java.util.ArrayList[Object]())

    // Cleanup
    table.close()
    connection.close()
    spark.stop()
  }
}
