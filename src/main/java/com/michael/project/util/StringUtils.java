package com.michael.project.util;

/**
 * 字符串工具类
 *
 * @author Michael Chu
 * @since 2020-01-06 17:37
 */
public class StringUtils {

    /**
     * 判断字符串是否为空
     *
     * @param str 字符串
     * @return 是否为空
     */
    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    /**
     * 判断字符串是否为空
     *
     * @param str 字符串
     * @return 是否不为空
     */
    public static boolean isNotEmpty(String str) {
        return str != null && !"".equals(str);
    }

    /**
     * 截断字符串两侧的逗号
     *
     * @param str 字符串
     * @return 字符串
     */
    public static String trimComma(String str) {
        if (str.startsWith(",")) {
            str = str.substring(1);
        }
        if (str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     *
     * @param str 待补全字符串
     * @return 字符串
     */
    public static String fulFill(String str) {
        if (str.length() == 2) {
            return str;
        } else {
            return "0" + str;
        }
    }

    /**
     * 从拼接的字符串中提取字段
     *
     * @param str 字符串
     * @param delimiter 分隔符
     * @param field 字段
     * @return 字段值
     */
    public static String getFieldFromConcatString(String str, String delimiter, String field) {
        try {
            String[] fields = str.split(delimiter);
            for (String concatField : fields) {
                if (concatField.split("=").length == 2) {
                    String fieldName = concatField.split("=")[0];
                    String fieldValue = concatField.split("=")[1];
                    if (fieldName.equals(field)) {
                        return fieldValue;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String setFieldInConcatString(String str, String delimiter, String field, String newFieldValue) {
        String[] fields = str.split(delimiter);
        return null;
    }

}