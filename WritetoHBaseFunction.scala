import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row, ForeachWriter}

def getHBaseForeachWriter(tableNameStr: String, rowKeyCol: String, columnFamily: String) = {
  new ForeachWriter[Row] {
    var connection: org.apache.hadoop.hbase.client.Connection = _
    var table: org.apache.hadoop.hbase.client.Table = _

    override def open(partitionId: Long, epochId: Long): Boolean = {
      val config = HBaseConfiguration.create()
      connection = ConnectionFactory.createConnection(config)
      table = connection.getTable(TableName.valueOf(tableNameStr))
      true
    }

    override def process(row: Row): Unit = {
      val rowKey = row.getAs[String](rowKeyCol)
      val put = new Put(Bytes.toBytes(rowKey))

      row.schema.fields.foreach { field =>
        val col = field.name
        if (col != rowKeyCol) {
          val value = Option(row.getAs[Any](col)).map(_.toString).getOrElse("")
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(col), Bytes.toBytes(value))
        }
      }

      table.put(put)
    }

    override def close(errorOrNull: Throwable): Unit = {
      if (table != null) table.close()
      if (connection != null) connection.close()
    }
  }
}


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Table}
import org.apache.hadoop.hbase.util.Bytes

def verifyRowInHBase(rowKey: String, tableName: String): Boolean = {
  val conf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val table: Table = connection.getTable(TableName.valueOf(tableName))

  try {
    // Get a row by its row key
    val get = new Get(Bytes.toBytes(rowKey))
    val result = table.get(get)
    
    // Check if the result contains data
    if (result.isEmpty) {
      println(s"Row with rowKey=$rowKey not found!")
      false
    } else {
      println(s"Row with rowKey=$rowKey found!")
      true
    }
  } finally {
    table.close()
    connection.close()
  }
}

val rowKey = "row1"
val tableName = "your_hbase_table"
val isInserted = verifyRowInHBase(rowKey, tableName)
println(s"Row inserted: $isInserted")


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.ResultScanner

def scanTable(tableName: String): Unit = {
  val conf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf(tableName))
  
  try {
    val scan = new Scan() // Scan the entire table
    val scanner: ResultScanner = table.getScanner(scan)

    scanner.forEach { result =>
      val rowKey = Bytes.toString(result.getRow)
      println(s"Row found: $rowKey")
    }
  } finally {
    connection.close()
  }
}

scanTable("your_hbase_table")






