
package swiss.army.knife.io


import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

import java.sql.{Connection, DriverManager, ResultSet}


/**
 * This class is used to Output DataFrame to Mysql.
 */
object DataFrameToMysql {


  /**
   * Insert `df` into `jdbcurl.tbname`.
   * @param hiveCtx HiveContext
   * @param df DataFrame contains all data needs to be inserted
   * @param jdbcurl JDBCURL contains mysql-info(host/port/user/passwd)
   * @param dbname Name of DataBase in Mysql(not used current)
   * @param tbname Name of TableName in Mysql.dbname
   * @param fields field list contains one row in Mysql 
   */
  def insertInto(hiveCtx: HiveContext,
                 df: DataFrame,
                 jdbcurl: String,
                 dbname: String,
                 tbname: String,
                 fields: List[Any]
                 ): Unit = {

    df.coalesce(10).registerTempTable(tbname)

    val sql_query = s"""
                    |select ${fields.mkString(", ")}
                    | from ${tbname}
                    |""".stripMargin.replaceAll("\n", "")
    println(sql_query)
    hiveCtx.sql(sql_query).
          coalesce(10).
          write.
          mode("append").
          jdbc(jdbcurl, tbname, new java.util.Properties())
    hiveCtx.dropTempTable(tbname)
  }


  /**
   * Insert `df` into `jdbcurl.tbname`.
   * @param hiveCtx HiveContext
   * @param df DataFrame contains all data needs to be inserted
   * @param jdbcurl JDBCURL contains mysql-info(host/port/user/passwd)
   * @param tbname Name of TableName in Mysql.dbname
   * @param fields field list contains one row in Mysql 
   */
  def insertInto(hiveCtx: HiveContext,
                 df: DataFrame,
                 jdbcurl: String,
                 tbname: String,
                 fields: List[Any]
                 ): Unit = {

    df.coalesce(10).registerTempTable(tbname)

    val sql_query = s"""
                    |select ${fields.mkString(", ")}
                    | from ${tbname}
                    |""".stripMargin.replaceAll("\n", "")
    println(sql_query)
    hiveCtx.sql(sql_query).
          coalesce(10).
          write.
          mode("append").
          jdbc(jdbcurl, tbname, new java.util.Properties())
    hiveCtx.dropTempTable(tbname)
  }


  /**
   * Insert `df` into `jdbcurl.tbname`.
   * @param hiveCtx HiveContext
   * @param df DataFrame contains all data needs to be inserted
   * @param jdbcurl JDBCURL contains mysql-info(host/port)
   * @param user UserName
   * @param passwd Password
   * @param dbname Name of DataBase in Mysql(not used current)
   * @param tbname Name of TableName in Mysql.dbname
   * @param fields field list contains one row in Mysql 
   * @param batchSize num of rows inserted one time
   */
  def insertInto(hiveCtx: HiveContext,
                 df: DataFrame,
                 jdbcurl: String,
                 user: String,
                 passwd: String,
                 dbname: String,
                 tbname: String,
                 fields: List[Any],
                 batchSize: Int
                 ): Unit = {

    df.coalesce(10).foreachPartition { iter =>
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection(jdbcurl)
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      val sql_query = s"""
                    |INSERT INTO ${tbname} (${fields.mkString(", ")})
                    | VALUES (${fields.map(_ => "?").mkString(", ")})
                    |""".stripMargin.replaceAll("\n", "")
      val prep = conn.prepareStatement(sql_query)

      try {
        var cnt_field = 0
        var cnt_row = 0
        for(r <- iter) {
          cnt_field = 0
          while(cnt_field < r.length) {
            prep.setObject(cnt_field + 1, r(cnt_field))
            cnt_field += 1
          }
          prep.addBatch
          cnt_row += 1

          if(cnt_row >= batchSize) {
            prep.executeBatch
            cnt_row = 0
          }
        }
        prep.executeBatch
      }
      finally {
        conn.close
      }
    }
  }


  /**
   * Insert `df` into `jdbcurl.tbname`.
   * Without all fields in mysql, use insert into tbname(c1, c2, ...) values (), especially when '=' in passwd
   * @param df DataFrame contains all data needs to be inserted
   * @param jdbcurl JDBCURL contains mysql-info(host/port)
   * @param tbname Name of TableName in Mysql.dbname
   * @param user User Name
   * @param passwd Password
   * @param df_fields field list contains one row(value in dataframe) in Mysql 
   * @param batchSize num of rows inserted one time
   */
  def insertInto(df: DataFrame,
                 jdbcurl: String,
                 tbname: String,
                 user: String,
                 passwd: String,
                 df_fields: List[Any],
                 batchSize: Int
                 ): Unit = {

    df.coalesce(10).foreachPartition { iter =>
      Class.forName("com.mysql.jdbc.Driver")
      val prop = new java.util.Properties
      prop.setProperty("user", user)
      prop.setProperty("password", passwd)
      val conn = DriverManager.getConnection(jdbcurl, prop)
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      val sql_query = s"""
                    |INSERT INTO ${tbname} (${df_fields.mkString(", ")})
                    | VALUES (${df_fields.map(_ => "?").mkString(", ")})
                    |""".stripMargin.replaceAll("\n", "")
      val prep = conn.prepareStatement(sql_query)

      try {
        var cnt_field = 0
        var cnt_row = 0
        for(r <- iter) {
          cnt_field = 0
          while(cnt_field < r.length) {
            prep.setObject(cnt_field + 1, r(cnt_field))
            cnt_field += 1
          }
          prep.addBatch
          cnt_row += 1

          if(cnt_row >= batchSize) {
            prep.executeBatch
            cnt_row = 0
          }
        }
        prep.executeBatch
      }
      finally {
        conn.close
      }
    }
  }


  /**
   * Insert `df` into `jdbcurl.tbname`.
   * Without all fields in mysql, use insert into tbname(c1, c2, ...) values ()
   * @param df DataFrame contains all data needs to be inserted
   * @param jdbcurl JDBCURL contains mysql-info(host/port/user/passwd)
   * @param tbname Name of TableName in Mysql.dbname
   * @param df_fields field list contains one row(value in dataframe) in Mysql 
   * @param const_fields field list contains one row(values are const_values) in Mysql 
   * @param const_values const values for const_fields in Mysql 
   * @param batchSize num of rows inserted one time
   */
  def insertInto(df: DataFrame,
                 jdbcurl: String,
                 tbname: String,
                 df_fields: List[Any],
                 const_fields: List[Any],
                 const_values: List[Any],
                 batchSize: Int
                 ): Unit = {

    df.coalesce(10).foreachPartition { iter =>
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection(jdbcurl)
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      val sql_query = s"""
                    |INSERT INTO ${tbname} (${df_fields.mkString(", ")}, ${const_fields.mkString(", ")})
                    | VALUES (${df_fields.map(_ => "?").mkString(", ")}, ${const_values.mkString(", ")})
                    |""".stripMargin.replaceAll("\n", "")
      val prep = conn.prepareStatement(sql_query)

      try {
        var cnt_field = 0
        var cnt_row = 0
        for(r <- iter) {
          cnt_field = 0
          while(cnt_field < r.length) {
            prep.setObject(cnt_field + 1, r(cnt_field))
            cnt_field += 1
          }
          prep.addBatch
          cnt_row += 1

          if(cnt_row >= batchSize) {
            prep.executeBatch
            cnt_row = 0
          }
        }
        prep.executeBatch
      }
      finally {
        conn.close
      }
    }
  }


  /**
   * Insert `df` into `jdbcurl.tbname`.
   * Without all fields in mysql, use insert into tbname(c1, c2, ...) values (), especially when '=' in passwd
   * @param df DataFrame contains all data needs to be inserted
   * @param jdbcurl JDBCURL contains mysql-info(host/port)
   * @param tbname Name of TableName in Mysql.dbname
   * @param user User Name
   * @param passwd Password
   * @param df_fields field list contains one row(value in dataframe) in Mysql 
   * @param const_fields field list contains one row(values are const_values) in Mysql 
   * @param const_values const values for const_fields in Mysql 
   * @param batchSize num of rows inserted one time
   */
  def insertInto(df: DataFrame,
                 jdbcurl: String,
                 tbname: String,
                 user: String,
                 passwd: String,
                 df_fields: List[Any],
                 const_fields: List[Any],
                 const_values: List[Any],
                 batchSize: Int
                 ): Unit = {

    df.coalesce(10).foreachPartition { iter =>
      Class.forName("com.mysql.jdbc.Driver")
      val prop = new java.util.Properties
      prop.setProperty("user", user)
      prop.setProperty("password", passwd)
      val conn = DriverManager.getConnection(jdbcurl, prop)
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      val sql_query = s"""
                    |INSERT INTO ${tbname} (${df_fields.mkString(", ")}, ${const_fields.mkString(", ")})
                    | VALUES (${df_fields.map(_ => "?").mkString(", ")}, ${const_values.mkString(", ")})
                    |""".stripMargin.replaceAll("\n", "")
      val prep = conn.prepareStatement(sql_query)

      try {
        var cnt_field = 0
        var cnt_row = 0
        for(r <- iter) {
          cnt_field = 0
          while(cnt_field < r.length) {
            prep.setObject(cnt_field + 1, r(cnt_field))
            cnt_field += 1
          }
          prep.addBatch
          cnt_row += 1

          if(cnt_row >= batchSize) {
            prep.executeBatch
            cnt_row = 0
          }
        }
        prep.executeBatch
      }
      finally {
        conn.close
      }
    }
  }

}

