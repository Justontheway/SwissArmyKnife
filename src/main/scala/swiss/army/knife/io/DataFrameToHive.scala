
package swiss.army.knife.io


import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext


/**
 * This class is used to Output DataFrame to Hive.
 */
object DataFrameToHive {


  /**
   * Insert `df` into `hiveCtx.dbname.tbname`.
   * @param hiveCtx HiveContext of the target Hive Dst
   * @param df DataFrame contains all data needs to be inserted
   * @param dbname Name of DataBase in hive
   * @param tbname Name of TableName in hive.dbname
   * @param fields field list contains one row in hive
   */
  def insertInto(hiveCtx: HiveContext,
                 df: DataFrame,
                 dbname: String,
                 tbname: String,
                 fields: List[Any]
                 ): Unit = {

    df.coalesce(10).registerTempTable(tbname)

    hiveCtx.sql(s"""
          |select ${fields.mkString(", ")}
          | from ${tbname}
          |""".stripMargin.replaceAll("\n", "")
          ).
          coalesce(10).
          write.
          mode("append").
          insertInto(s"${dbname}.${tbname}")

    hiveCtx.dropTempTable(tbname)
  }


  /**
   * Insert `df` into `hiveCtx.dbname.tbname` with partition info.
   * @param hiveCtx HiveContext of the target Hive Dst
   * @param df DataFrame contains all data needs to be inserted
   * @param dbname Name of DataBase in hive
   * @param tbname Name of TableName in hive.dbname
   * @param fields field list contains one row in hive
   * @param partitions partition list in hive
   */
  def insertInto(hiveCtx: HiveContext,
                 df: DataFrame,
                 dbname: String,
                 tbname: String,
                 fields: List[Any],
                 partitions: List[Any]
                 ): Unit = {

    df.coalesce(10).registerTempTable(tbname)

    hiveCtx.sql(s"""
          |insert into table ${dbname}.${tbname} partition(${partitions.mkString(" and ")})
          | select ${fields.mkString(", ")}
          | from ${tbname}
          |""".stripMargin.replaceAll("\n", "")
          )

    hiveCtx.dropTempTable(tbname)
  }

}
