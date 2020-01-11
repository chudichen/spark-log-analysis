package com.michael.project.util;

/**
 * 校验工具类
 *
 * @author Michael Chu
 * @since 2020-01-10 11:12
 */
public class ValidUtils {

    /**
     * 校验数据中的指定字段，是否在指定范围内
     *
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param startParamField 起始参数字段
     * @param endParamField 结束参数段
     * @return 校验结果
     */
    public static boolean between(String data, String dataField,
                                  String parameter, String startParamField, String endParamField) {
        String startParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", startParamField);
        String endParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField);
        if (StringUtils.isEmpty(startParamFieldStr) || StringUtils.isEmpty(endParamFieldStr)) {
            return true;
        }

        int startParamFieldValue = Integer.parseInt(startParamFieldStr);
        int endParamFieldValue = Integer.parseInt(endParamFieldStr);

        String dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField);
        if (dataFieldStr != null) {
            int dataFieldValue = Integer.parseInt(dataField);
            return dataFieldValue >= startParamFieldValue && dataFieldValue <= endParamFieldValue;
        }
        return false;
    }

    /**
     * 校验数据中的指定字段，是否有值与参数字段的值相同
     *
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean in(String data, String dataField, String parameter, String paramField) {
        String paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField);
        if (paramFieldValue == null) {
            return true;
        }
        String[] paramFieldValueSplit = paramFieldValue.split(",");

        String dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField);
        if (dataFieldValue != null) {
            String[] dataFieldValueSplit = dataFieldValue.split(",");

            for (String singleDataFieldValue : dataFieldValueSplit) {
                for (String singleParamFieldValue : paramFieldValueSplit) {
                    if (singleDataFieldValue.equals(singleParamFieldValue)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * 校验数据中的指定字段，是否在指定范围内
     *
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean equal(String data, String dataField, String parameter, String paramField) {
        String paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField);
        if (paramFieldValue == null) {
            return true;
        }

        String dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField);
        return dataFieldValue != null;
    }

}
