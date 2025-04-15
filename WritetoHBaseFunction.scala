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
