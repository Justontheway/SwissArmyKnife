
package swiss.army.knife.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Table, ConnectionFactory, Connection}
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._


/**
 * This class is used to output DataFrame to Hbase.
 */
object DataFrameToHbase {

  /**
   * Insert `dataFrame` into hbase table `tableName`.<br>
   * Insert each partition one time.
   * @param dataFrame DataFrame contains all data needs to be inserted
   * @param tableName tablename in Hbase
   * @param family family name in Hbase.tableName
   * @param qualifier qualifier name in Hbase.tableName.family
   */
  def insertInto(dataFrame: DataFrame, tableName: String, family: String, qualifier: String) : Unit = {
    dataFrame.foreachPartition(partitionOfRecords => {
      // 每分区一个hbase连接
      val hConfig: Configuration = HBaseConfiguration.create()
      val connection: Connection = ConnectionFactory.createConnection(hConfig)
      val table: Table = connection.getTable(TableName.valueOf(tableName))
      var puts: ListBuffer[Put] = ListBuffer[Put]()

      try {
        partitionOfRecords.foreach(row => {
          val put : Put = new Put(Bytes.toBytes(row(0).toString))
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(row(1).toString))
          puts += put
        })
        table.put(puts.toList.asJava)
      } catch {
        case e: Exception => e.printStackTrace
      }
      finally {
        if(table != null){
          table.close()}
        if(connection != null){
          connection.close()}
      }
    })
  }


  /**
   * Insert `dataFrame` into hbase table `tableName`.<br>
   * Insert <=partLen one time.
   * @param dataFrame DataFrame contains all data needs to be inserted
   * @param tableName tablename in Hbase
   * @param family family name in Hbase.tableName
   * @param qualifier qualifier name in Hbase.tableName.family
   * @param partLen count of rows inserted into Hbase one time
   */
  def insertInto(dataFrame: DataFrame, tableName: String, family: String, qualifier: String, partLen: Int) : Unit = {
    dataFrame.foreachPartition(partitionOfRecords => {
      // 每分区一个hbase连接
      val hConfig: Configuration = HBaseConfiguration.create()
      val connection: Connection = ConnectionFactory.createConnection(hConfig)
      val table: Table = connection.getTable(TableName.valueOf(tableName))
      var puts: ListBuffer[Put] = ListBuffer[Put]()
      var cnt: Int = 0

      try {
        while(partitionOfRecords.hasNext) {
          val row = partitionOfRecords.next
          val put : Put = new Put(Bytes.toBytes(row(0).toString))
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(row(1).toString))
          puts += put
          cnt = cnt + 1
          if(cnt == partLen) {
            table.put(puts.toList.asJava)
            puts = ListBuffer[Put]()
            cnt = 0
          }
        }
        table.put(puts.toList.asJava)
      } catch {
        case e: Exception => e.printStackTrace
      }
      finally {
        if(table != null){
          table.close()}
        if(connection != null){
          connection.close()}
      }
    })
  }

}

