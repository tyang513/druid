package com.talkingdata.olap.druid.post;

import org.apache.commons.lang3.StringUtils;

/**
 * CardinalityFunc
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-25
 */
public enum CardinalityFunc {
    DISCRETE,
    COMBINE,
    TOPN;

    public static CardinalityFunc fromValue(String value) {
        if (StringUtils.isNotEmpty(value)) {
            CardinalityFunc func = valueOf(value.toUpperCase());
            if (func != null) {
                return func;
            }
        }
        return COMBINE;
    }
}
