
package swiss.army.knife

import org.apache.spark.sql.DataFrame


/**
 * This class is used to manipulate sets of things.
 */
object SetUtil {

  /**
   * 计算Array中所有DataFrame集合的并集,并打印并集元素的个数.<br>
   * @param df_arr 同结构的DataFrame形成的集合
   * @return return the full set of in-sets
   */
  def dfFullSet(df_arr: Array[DataFrame]): DataFrame = {
    var df_tmp = df_arr(0)
    for(pos <- 1 until df_arr.length) {
      df_tmp = df_tmp.unionAll(df_arr(pos))
    }

    println(df_tmp.count)
    df_tmp
  }

  /**
   * 计算Array中所有DataFrame集合两两之间的交集元素的个数,并打印有交集信息<br>
   * @param df_arr 同结构的DataFrame形成的集合
   * @return return the set-intersection for any two sets as an Array.
   */
  def dfIntersectionSet(df_arr: Array[DataFrame]): Array[(Int, Int, Long)] = {
    val cnt_result = new Array[(Int, Int, Long)](df_arr.length * (df_arr.length-1) / 2)
    var num = 0
    for(i <- 0 until df_arr.length) {
      for(j <- i+1 until df_arr.length) {
        cnt_result(num) = (i+1, j+1, df_arr(i).intersect(df_arr(j)).count)
        num += 1
      }
    }

    for(i <- cnt_result if i._3 > 0) {
      println(i)
    }

    cnt_result
  }

  /**
   * Array中所有DataFrame集合分别与df_single交集元素的个数,并打印交集信息.<br>
   * @param df_arr 同一结构的DataFrame集合
   * @param df_single 同df_arr元素同结构的DataFrame
   * @return return count of set intersection
   */
  def dfIntersectionSet(df_arr: Array[DataFrame], df_single: DataFrame): Array[Long] = {
    val cnt_result = new Array[Long](df_arr.length)

    for(pos <- 0 until df_arr.length) {
        cnt_result(pos) = df_arr(pos).intersect(df_single).count
    }

    for(i <- cnt_result if i > 0) {
      println(i)
    }

    cnt_result
  }

}

