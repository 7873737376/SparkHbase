import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import util.HBaseHelper

import scala.collection.JavaConverters._

object BatchCallbackExampleScala {

  val ROW1 = Bytes.toBytes("row1")
  val ROW2 = Bytes.toBytes("row2")
  val COLFAM1 = Bytes.toBytes("colfam1")
  val COLFAM2 = Bytes.toBytes("colfam2")
  val QUAL1 = Bytes.toBytes("qual1")
  val QUAL2 = Bytes.toBytes("qual2")

  def main(args: Array[String]): Unit = {
    val conf: Configuration = HBaseConfiguration.create()

    val helper = HBaseHelper.getHelper(conf)
    helper.dropTable("testtable")
    helper.createTable("testtable", "colfam1", "colfam2")
    helper.put(
      "testtable",
      Array("row1"),
      Array("colfam1"),
      Array("qual1", "qual2", "qual3"),
      Array(1L, 2L, 3L),
      Array("val1", "val2", "val3")
    )

    println("Before batch call...")
    helper.dump("testtable", Array("row1", "row2"), null, null)

    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("testtable"))

    val batch = new java.util.ArrayList[Row]()

    val put = new Put(ROW2)
    put.addColumn(COLFAM2, QUAL1, 4L, Bytes.toBytes("val5"))
    batch.add(put)

    val get1 = new Get(ROW1)
    get1.addColumn(COLFAM1, QUAL1)
    batch.add(get1)

    val delete = new Delete(ROW1)
    delete.addColumns(COLFAM1, QUAL2)
    batch.add(delete)

    val get2 = new Get(ROW2)
    get2.addFamily(Bytes.toBytes("BOGUS"))
    batch.add(get2)

    val results = new Array[Object](batch.size())

    try {
      table.batchCallback(batch, results, new Batch.Callback[Result] {
        override def update(region: Array[Byte], row: Array[Byte], result: Result): Unit = {
          println(s"Received callback for row[${Bytes.toString(row)}] -> $result")
        }
      })
    } catch {
      case e: Exception =>
        System.err.println("Error: " + e)
    }

    for (i <- results.indices) {
      val result = results(i)
      println(s"Result[$i]: type = ${result.getClass.getSimpleName}; $result")
    }

    table.close()
    connection.close()
    println("After batch call...")
    helper.dump("testtable", Array("row1", "row2"), null, null)
    helper.close()
  }
}
