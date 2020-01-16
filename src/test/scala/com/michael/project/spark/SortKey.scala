package com.michael.project.spark

/**
 * @author Michael Chu
 * @since 2020-01-16 16:39
 */
class SortKey(val clickCount: Int, val orderCount: Int, val payCount: Int) extends Ordered[SortKey] with Serializable {

  override def compare(that: SortKey): Int = {
    if (clickCount - that.clickCount != 0) {
      clickCount - that.clickCount
    } else if (orderCount - that.orderCount != 0) {
      orderCount - that.orderCount
    } else if (payCount - that.payCount != 0) {
      payCount - that.payCount
    } else {
      0
    }
  }
}
