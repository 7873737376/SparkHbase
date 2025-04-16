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
