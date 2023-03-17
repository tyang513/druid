package com.talkingdata.olap.druid.serde;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;

/**
 * AtomCubeAggregator
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class AtomCubeBufferAggregator implements BufferAggregator {
    private final Map<Integer, Aggregator> cache;
    private final AggregatorFactory aggregatorFactory;
    private final ColumnSelectorFactory selectorFactory;

    public AtomCubeBufferAggregator(AggregatorFactory aggregatorFactory, ColumnSelectorFactory selectorFactory) {
        this.aggregatorFactory = aggregatorFactory;
        this.selectorFactory = selectorFactory;
        this.cache = new HashMap<>();
    }

    @Override
    public void init(final ByteBuffer buf, final int position) {
        cache.put(position, aggregatorFactory.factorize(selectorFactory));
    }

    @Override
    public final void aggregate(ByteBuffer buf, int position) {
        cache.get(position).aggregate();
    }

    @Nullable
    @Override
    public final Object get(ByteBuffer buf, int position) {
        return cache.get(position).get();
    }

    @Override
    public final float getFloat(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("Unsupported method: getFloat");
    }

    @Override
    public final long getLong(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("Unsupported method: getLong");
    }

    @Override
    public double getDouble(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("Unsupported method: getDouble");
    }

    @Override
    public void close() {
    }
}
