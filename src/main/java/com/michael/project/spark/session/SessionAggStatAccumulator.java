package com.michael.project.spark.session;

import com.michael.project.constant.Constants;
import com.michael.project.util.StringUtils;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.util.AccumulatorV2;

/**
 * session聚合统计Accumulator
 *
 * 大家可以看到
 * 其实使用自己定义的一些数据格式，比如String，甚至说，我们可以自己定义model，自己定义的类（必须可序列化）
 * 然后呢，可以基于这种特殊的数据格式，可以实现自己复杂的分布式的计算逻辑
 * 各个task，分布式在运行，可以根据你的需求，task给Accumulator传入不同的值
 * 根据不同的值，去作复杂的逻辑
 *
 * Spark Core里面很实用的高端技术
 *
 * @author Michael Chu
 * @since 2020-01-09 17:12
 */
public class SessionAggStatAccumulator extends AccumulatorV2<String, String> {

    private static final long serialVersionUID = 6311074555136039130L;

    private String value = INIT_VALUE;

    private static final String INIT_VALUE = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    @Override
    public boolean isZero() {
        return INIT_VALUE.equals(value);
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        return null;
    }

    @Override
    public void reset() {
        this.value = INIT_VALUE;
    }

    @Override
    public void add(String v) {
        if(StringUtils.isEmpty(v)) {
            return;
        }


    }

    @Override
    public void merge(AccumulatorV2 other) {

    }

    @Override
    public String value() {
        return this.value;
    }
}
