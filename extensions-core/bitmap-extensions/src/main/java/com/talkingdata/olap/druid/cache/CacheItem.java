package com.talkingdata.olap.druid.cache;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;

/**
 * CacheItem
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-26
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CacheItem<T extends Serializable> implements Serializable {
    private long created = System.currentTimeMillis();
    private T value;

    public CacheItem(T value) {
        this.value = value;
    }

    public CacheItem() {
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
