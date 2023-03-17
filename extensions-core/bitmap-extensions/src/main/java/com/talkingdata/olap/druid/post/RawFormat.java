package com.talkingdata.olap.druid.post;

import org.apache.commons.lang3.StringUtils;

/**
 * RawFormat
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public enum RawFormat {
    LIST,
    ROARINGBASE64;

    public static RawFormat fromValue(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        return value.toUpperCase().equals(ROARINGBASE64.name()) ? ROARINGBASE64 : LIST;
    }
}
