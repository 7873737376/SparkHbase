1. Writes Are in MemStore (Not Yet Flushed to Disk)
HBase buffers writes in memory (MemStore). Until flushed (automatically or manually), you won’t see the records in a scan.

Solution: Flush the table programmatically

Use HBase Admin.flush() method after the streaming write is complete.

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName

def flushTable(tableName: String): Unit = {
  val conf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val admin = connection.getAdmin
  try {
    admin.flush(TableName.valueOf(tableName))
    println(s"Flush triggered for $tableName")
  } finally {
    admin.close()
    connection.close()
  }
}
Note: Do this after writing a batch or periodically (not after every single record). In streaming, you can hook this into a custom trigger or do it via a separate thread if needed.
2. Data Not Written at All
If flushing doesn’t help, it’s possible the data isn’t being written correctly.

Double-check:

Row key exists and is non-null.
Put object is populated correctly.
Table exists, and auto-flush is not disabled (should be on for streaming).
Table schema matches your expectations (e.g., column families personal, work, etc.).
Add debug prints inside the .process() method:

println(s"Writing rowKey: $rowKey, data: $put")
3. Enable Auto-Flush (if needed)
HTable by default uses auto-flush in newer HBase clients. If you're using low-level APIs, ensure:

table.setAutoFlush(true)
Though this is rare in modern APIs — just ensure you're calling table.put(put) properly.

4. HBase Compaction Delay
After flushing, data should be visible. But if you do lots of small writes, a major compaction may delay their visibility across RegionServers. Use:

major_compact 'your_table'
in the HBase shell to force it.

5. Verify Data Using Scan
After a flush, run this in HBase shell:

scan 'your_hbase_table'
Or scan by prefix/row key:

get 'your_hbase_table', 'row-key-value'
Want to Trigger Flush Automatically?
For streaming, use a time-based flush trigger:

spark.streams.addListener(new StreamingQueryListener {
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    flushTable("your_table")
  }
  override def onQueryStarted(event: QueryStartedEvent): Unit = {}
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
})
Let me know:

Which HBase version you're on?
Whether you're using HTable, BufferedMutator, or high-level connector?
And how you validated the data (HBase shell, scan, REST API, etc.)?
Then I can fine-tune the flush logic for your setup.


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQueryListener, QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

// Define flush function
def flushTable(tableName: String): Unit = {
  val conf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val admin = connection.getAdmin
  try {
    admin.flush(TableName.valueOf(tableName))
    println(s"Flush triggered for $tableName")
  } finally {
    admin.close()
    connection.close()
  }
}

// Create Spark session
val spark = SparkSession.builder()
  .appName("StructuredStreamingToHBase")
  .master("local[*]") // remove in production
  .getOrCreate()

// Add HBase flush listener
spark.streams.addListener(new StreamingQueryListener {
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println(s"Query started: ${event.id}")
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    println(s"Query made progress: ${event.progress}")
    flushTable("your_hbase_table") // Replace with your HBase table
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    println(s"Query terminated: ${event.id}")
  }
})

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Admin}

def majorCompactTable(tableName: String): Unit = {
  val conf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val admin: Admin = connection.getAdmin

  try {
    admin.majorCompact(TableName.valueOf(tableName))
    println(s"Major compaction triggered for table: $tableName")
  } finally {
    admin.close()
    connection.close()
  }
}


