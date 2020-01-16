package com.michael.project.spark.session;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.Serializable;
import scala.math.Ordered;

import java.util.Objects;

/**
 * 品类二次排序key
 * <p>
 * 封装你要进行排序算法需要的几个字段：点击次数、下单次数和支付次数
 * 实现Ordered接口要求的几个方法
 * <p>
 * 跟其他key相比，如何来判定大于、大于等于、小于
 * <p>
 * 依次使用三个次数进行进行比较，如果某一个相等，那么就比较下一个
 *
 * @author Michael Chu
 * @since 2020-01-15 16:50
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private static final long serialVersionUID = -6007890914324789180L;

    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    @Override
    public boolean $greater(CategorySortKey other) {
        if (clickCount > other.getClickCount()) {
            return true;
        } else if (Objects.equals(clickCount, other.getClickCount()) &&
                orderCount > other.getOrderCount()) {
            return true;
        } else if (Objects.equals(clickCount, other.getClickCount()) &&
                Objects.equals(orderCount, other.getOrderCount()) &&
                payCount > other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if ($greater(other)) {
            return true;
        } else if (Objects.equals(clickCount, other.getClickCount()) &&
                Objects.equals(orderCount, other.getOrderCount()) &&
                Objects.equals(payCount, other.getPayCount())) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less(CategorySortKey other) {
        if (clickCount < other.getClickCount()) {
            return true;
        } else if (Objects.equals(clickCount, other.getClickCount()) &&
                orderCount < other.getOrderCount()) {
            return true;
        } else if (Objects.equals(clickCount, other.getClickCount()) &&
                Objects.equals(orderCount, other.getOrderCount()) &&
                payCount < other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if ($less(other)) {
            return true;
        } else if (Objects.equals(clickCount, other.getClickCount()) &&
                Objects.equals(orderCount, other.getOrderCount()) &&
                Objects.equals(payCount, other.getPayCount())) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(CategorySortKey other) {
        if (clickCount - other.getClickCount() != 0) {
            return (int) (clickCount - other.getClickCount());
        } else if (orderCount - other.getOrderCount() != 0) {
            return (int) (orderCount - other.getOrderCount());
        } else if (payCount - other.getPayCount() != 0) {
            return (int) (payCount - other.getPayCount());
        }
        return 0;
    }

    @Override
    public int compareTo(CategorySortKey other) {
        if (clickCount - other.getClickCount() != 0) {
            return (int) (clickCount - other.getClickCount());
        } else if (orderCount - other.getOrderCount() != 0) {
            return (int) (orderCount - other.getOrderCount());
        } else if (payCount - other.getPayCount() != 0) {
            return (int) (payCount - other.getPayCount());
        }
        return 0;
    }
}
