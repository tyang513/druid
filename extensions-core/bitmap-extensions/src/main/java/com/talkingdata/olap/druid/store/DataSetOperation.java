package com.talkingdata.olap.druid.store;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * DataSetOperation
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-19
 */
public enum DataSetOperation {
    UNION,
    INTERSECT,
    NOT,
    DELETE;

    private static final Map<String, DataSetOperation> MAP;

    static {
        MAP = new HashMap<>();
        for (DataSetOperation item : DataSetOperation.values()) {
            MAP.put(item.name(), item);
        }
    }

    public static DataSetOperation fromValue(String name) {
        return StringUtils.isEmpty(name) ? null : MAP.get(name.toUpperCase());
    }
}
