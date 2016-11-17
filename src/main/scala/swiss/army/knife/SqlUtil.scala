
package swiss.army.knife

/**
 * This class is used to generate part of sql string.
 */
object SqlUtil {

  /**
   * 组合year,month,day分区字段为某一天之后.<br>
   * @param someday string of datetime
   * @return return partitial sql string
   */
  def partition_after_someday(someday: String): String = {
    val year_someday = someday.substring(0, 4)
    val month_someday = someday.substring(5, 7)
    val day_someday = someday.substring(8)

    s""" (concat(year, '-', month, '-', day) > ${someday}) """
    s"""
      | ((
      | year = '${year_someday}'
      | and month = '${month_someday}'
      | and day > '${day_someday}'
      | )
      | or
      | (
      | year = '${year_someday}'
      | and month > '${month_someday}'
      | )
      | or
      | (
      | year > '${year_someday}'
      | ))
    |""".stripMargin.replaceAll("\n", "")
  }


  /**
   * 组合year,month,day分区字段为某一天或之后.<br>
   * @param someday string of datetime
   * @return return partitial sql string
   */
  def partition_after_eq_someday(someday: String): String = {
    val year_someday = someday.substring(0, 4)
    val month_someday = someday.substring(5, 7)
    val day_someday = someday.substring(8)

    s""" (concat(year, '-', month, '-', day) > ${someday}) """
    s"""
      | ((
      | year = '${year_someday}'
      | and month = '${month_someday}'
      | and day >= '${day_someday}'
      | )
      | or
      | (
      | year = '${year_someday}'
      | and month > '${month_someday}'
      | )
      | or
      | (
      | year > '${year_someday}'
      | ))
    |""".stripMargin.replaceAll("\n", "")
  }


  /**
   * 组合year,month,day分区字段为某一天之前.<br>
   * @param someday string of datetime
   * @return return partitial sql string
   */
  def partition_before_someday(someday: String): String = {
    val year_someday = someday.substring(0, 4)
    val month_someday = someday.substring(5, 7)
    val day_someday = someday.substring(8)

    s""" (concat(year, '-', month, '-', day) < ${someday}) """
    s"""
      | ((
      | year = '${year_someday}'
      | and month = '${month_someday}'
      | and day < '${day_someday}'
      | )
      | or
      | (
      | year = '${year_someday}'
      | and month < '${month_someday}'
      | )
      | or
      | (
      | year < '${year_someday}'
      | ))
    |""".stripMargin.replaceAll("\n", "")
  }


  /**
   * 组合year,month,day分区字段为某一天或之前.<br>
   * @param someday string of datetime
   * @return return partitial sql string
   */
  def partition_before_eq_someday(someday: String): String = {
    val year_someday = someday.substring(0, 4)
    val month_someday = someday.substring(5, 7)
    val day_someday = someday.substring(8)

    s""" (concat(year, '-', month, '-', day) <= ${someday}) """
    s"""
      | ((
      | year = '${year_someday}'
      | and month = '${month_someday}'
      | and day <= '${day_someday}'
      | )
      | or
      | (
      | year = '${year_someday}'
      | and month < '${month_someday}'
      | )
      | or
      | (
      | year < '${year_someday}'
      | ))
    |""".stripMargin.replaceAll("\n", "")
  }


  /**
   * 组合year,month,day分区字段为某一天.<br>
   * @param someday string of datetime
   * @return return partitial sql string
   */
  def partition_on_someday(someday: String): String = {
    val year_someday = someday.substring(0, 4)
    val month_someday = someday.substring(5, 7)
    val day_someday = someday.substring(8)

    s""" (concat(year, '-', month, '-', day) = ${someday}) """
    s"""
      | (
      | year = '${year_someday}'
      | and month = '${month_someday}'
      | and day = '${day_someday}'
      | )
    |""".stripMargin.replaceAll("\n", "")
  }

}
