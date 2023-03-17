package com.talkingdata.olap.druid.serde;

import javax.annotation.Nullable;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.roaringbitmap.RoaringBitmap;

/**
 * AtomCubeAggregator
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class AtomCubeAggregator implements Aggregator {
    private final BaseObjectColumnValueSelector selector;
    private final RoaringBitmap bitmap;

    public AtomCubeAggregator(BaseObjectColumnValueSelector selector) {
        this.selector = selector;
        this.bitmap = new RoaringBitmap();
    }

    @Override
    public void aggregate() {
        Object object = selector.getObject();
        if (object == null) {
            return;
        }

        bitmap.or((RoaringBitmap) object);
    }

    @Nullable
    @Override
    public Object get() {
        return bitmap;
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException("Unsupported method: getFloat");
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException("Unsupported method: getLong");
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException("Unsupported method: getDouble");
    }

    @Override
    public void close() {
    }
}
