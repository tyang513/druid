package com.talkingdata.olap.druid.post;

import org.apache.commons.lang3.StringUtils;

/**
 * DataSetFunc
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-09
 */
public enum DataSetFunc {
    UNION,
    INTERSECT,
    NOT,
    FULL_INTERSECT,
    FULL_UNION,
    FULL_NOT;

    public static DataSetFunc fromValue(String value) {
        if (StringUtils.isNotEmpty(value)) {
            DataSetFunc func = valueOf(value.toUpperCase());
            if (func != null) {
                return func;
            }
        }
        return UNION;
    }

    public static boolean supportGroupBy(String value) {
        if (StringUtils.isEmpty(value)) {
            return false;
        }

        String upperValue = value.toUpperCase();
        return upperValue.equals(FULL_INTERSECT.name())
                || upperValue.equals(FULL_UNION.name())
                || upperValue.equals(FULL_NOT.name());
    }
}
