
package swiss.army.knife.stat

import org.apache.spark.sql.{DataFrame, Row}

import swiss.army.knife.util.Logging

/**
 * This object is used to offer some useful functions for
 * data stating.
 */
object StatFunction extends Logging {

  /**
   * Calculate the quantiles of given dataset df with index-order.
   * Don't try to calculate fraction 0.0.</br>
   * @param df DataFrame contains all the dataset
   * @param frac A set of fractions used to calculate quantiles
   * @param sorted Whether the given `df` is sorted
   */
  def calculateQuantileWithIndex(df: DataFrame, frac: Seq[Double],
      sorted: Boolean = false): Seq[(Row, Long)] = {

    val cnt = df.count
    var df_sorted = df

    logInfo(s"calculate quantile [${frac.mkString(",")}] of DataFrame[${df}].")
    if (!sorted) {
      logInfo(s"DataFrame[${df}] is not sorted, sort with first column.")
      df_sorted = df.orderBy(df.columns(0))
    }

    // zipWithIndex starts with 0
    df_sorted.rdd
      .zipWithIndex
      .filter(r => frac.map(f => (cnt * f).ceil.toLong - 1).contains(r._2))
      .collect
  }

  /**
   * Calculate the quantiles of given dataset df.
   * Don't try to calculate fraction 0.0.</br>
   * @param df DataFrame contains all the dataset
   * @param frac A set of fractions
   * @param sorted Whether the given `df` is sorted
   */
  def calculateQuantile(df: DataFrame, frac: Seq[Double],
      sorted: Boolean = false): Seq[Row] = {

    calculateQuantileWithIndex(df, frac, sorted).map(x => x._1)
  }

  /**
   * Calculate the quantiles of given dataset df with original fraction.
   * Don't try to calculate fraction 0.0.</br>
   * @param df DataFrame contains all the dataset
   * @param frac A set of fractions
   * @param sorted Whether the given `df` is sorted
   */
  def calculateQuantileWithFrac(df: DataFrame, frac: Seq[Double],
      sorted: Boolean = false): Seq[(Row, Double)] = {

    calculateQuantileWithIndex(df, frac, sorted)
      .sortBy(_._2)
      .map(x => x._1)
      .zip(frac.sorted)
  }
}

