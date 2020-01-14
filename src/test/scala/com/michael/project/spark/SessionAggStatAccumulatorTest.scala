package com.michael.project.spark

import com.michael.project.constant.Constants
import com.michael.project.spark.session.SessionAggStatAccumulator
import com.michael.project.util.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

/**
 * session聚合统计测试
 *
 * @author Michael Chu
 * @since 2020-01-13 10:34
 */
object SessionAggStatAccumulatorTest {

  val accumulator = new SessionAggStatAccumulator()

  def main(args: Array[String]): Unit = {
    object SessionAggStatAccumulator extends AccumulatorV2[String, String] {
      private var _value = init_value
      private val init_value = Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s+ "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0";

      override def isZero: Boolean = {
        init_value.equals(_value)
      }

      override def copy(): AccumulatorV2[String, String] = {
        new SessionAggStatAccumulator
      }

      override def reset(): Unit = {
        _value = init_value
      }

      override def add(v: String): Unit = {
        if (StringUtils.isEmpty(v)) return

        // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
        val oldValue = StringUtils.getFieldFromConcatString(_value, "\\|", v)
        if (oldValue != null) { // 将范围区间原有的值，累加1
          val newValue = oldValue.toInt + 1
          // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
          _value = StringUtils.setFieldInConcatString(_value, "\\|", v, String.valueOf(newValue))
        }
      }

      override def merge(other: AccumulatorV2[String, String]): Unit = {

      }

      override def value: String = {
        _value
      }
    }

    // 创建Spark上下文
    val conf = new SparkConf().setAppName("SessionAggStatAccumulatorTest").setMaster("local")
    val sc = new SparkContext(conf)
    sc.register(accumulator)

    val arr = Array(Constants.TIME_PERIOD_1s_3s, Constants.TIME_PERIOD_4s_6s)
    val rdd = sc.parallelize(arr, 1)
    rdd.foreach {accumulator.add}

    println(accumulator.value)
  }

}
