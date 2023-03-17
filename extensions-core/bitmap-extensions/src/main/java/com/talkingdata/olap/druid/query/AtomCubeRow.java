package com.talkingdata.olap.druid.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.talkingdata.olap.druid.BitmapUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * ResultView
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AtomCubeRow extends HashMap<String, Object> {
    public static final String VALUE_KEY = "_value";
    private static final String TIME_KEY = "timestamp";

    public AtomCubeRow() {
    }

    public AtomCubeRow(Map<? extends String, ?> map) {
        super(map);
    }

    public AtomCubeRow(String timestamp, MutableRoaringBitmap value) {
        put(TIME_KEY, timestamp);
        put(VALUE_KEY, value);
    }

    public AtomCubeRow(String timestamp, Map<? extends String, ?> map) {
        super(map);
        put(TIME_KEY, timestamp);
    }

    public MutableRoaringBitmap getBitmapValue() {
        return (MutableRoaringBitmap) getOrDefault(VALUE_KEY, BitmapUtil.EMPTY_BITMAP);
    }

    public int getCardinalityValue() {
        Object value = get(VALUE_KEY);
        if (value == null) {
            return 0;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        } else {
            return ((MutableRoaringBitmap) value).getCardinality();
        }
    }

    public void removeValue() {
        remove(VALUE_KEY);
    }

    public void setValue(Object value) {
        put(VALUE_KEY, value);
    }

    public void bitmapToCardinality() {
        MutableRoaringBitmap bitmap = getBitmapValue();
        put(VALUE_KEY, bitmap != null ? bitmap.getCardinality() : 0);
    }

    public String getTimestamp() {
        Object timestamp = get(TIME_KEY);
        return timestamp != null ? timestamp.toString() : null;
    }

    public Sequence<AtomCubeRow> toSequence() {
        return Sequences.simple(Collections.singletonList(this));
    }
}
