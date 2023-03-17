package com.talkingdata.olap.druid.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.talkingdata.olap.druid.BitmapUtil;
import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import com.talkingdata.olap.druid.cache.CacheKeyProvider;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NoopAggregator;
import org.apache.druid.query.aggregation.NoopBufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ValueType;
import org.roaringbitmap.RoaringBitmap;

/**
 * AtomCubeAggregator
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class AtomCubeAggregatorFactory extends AggregatorFactory {
    private static final DebugLogger TRACER = DebugLoggerFactory.getLogger(AtomCubeAggregatorFactory.class);

    private final String name;
    private final String fieldName;

    @JsonCreator
    public AtomCubeAggregatorFactory(@JsonProperty("name") String name, @JsonProperty("fieldName") String fieldName) {
        this.fieldName = fieldName;
        this.name = name == null ? this.fieldName : name;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {

        BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
        if (selector instanceof NilColumnValueSelector) {
            return NoopAggregator.instance();
        }

        final Class classOfObject = selector.classOfObject();
        if (classOfObject.equals(Object.class) || RoaringBitmap.class.isAssignableFrom(classOfObject)) {
            return new AtomCubeAggregator(selector);
        }

        throw new IAE("Incompatible type for metric[%s], expected a TMap, got a %s", fieldName, classOfObject);
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {

        BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
        if (selector instanceof NilColumnValueSelector) {
            return NoopBufferAggregator.instance();
        }

        final Class classOfObject = selector.classOfObject();
        if (classOfObject.equals(Object.class) || RoaringBitmap.class.isAssignableFrom(classOfObject)) {
            return new AtomCubeBufferAggregator(this, metricFactory);
        }

        throw new IAE("Incompatible type for metric[%s], expected a TDBitmap, got a %s", fieldName, classOfObject);
    }

    @Override
    public boolean canVectorize(ColumnInspector columnInspector) {
        return false;
    }

    @Override
    public Comparator getComparator() {
        return BitmapUtil.COMPARATOR;
    }

    @Override
    public Object combine(Object lhs, Object rhs) {
        if (lhs instanceof RoaringBitmap && rhs instanceof RoaringBitmap) {
            return BitmapUtil.union((RoaringBitmap) lhs, (RoaringBitmap) rhs);
        } else if (lhs == null) {
            return rhs;
        } else {
            return lhs;
        }
    }

    @Override
    public AggregateCombiner makeAggregateCombiner() {
        return new AtomCubeAggregateCombiner();
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new AtomCubeAggregatorFactory(name, name);
    }

    @Override
    public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException {
        if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
            return getCombiningFactory();
        } else {
            throw new AggregatorFactoryNotMergeableException(this, other);
        }
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(new AtomCubeAggregatorFactory(fieldName, fieldName));
    }

    @Override
    public Object deserialize(Object object) {

        if (object instanceof byte[]) {
            return BitmapUtil.fromBytes((byte[]) object);
        } else if (object instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) object).duplicate();
            buffer.limit(buffer.position() + buffer.remaining());
            RoaringBitmap roaringBitmap = BitmapUtil.fromBytes(buffer);
            TRACER.debug("Deserialized", this.hashCode(), roaringBitmap);
            return roaringBitmap;
        } else {
            return object;
        }
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object) {
        return object;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(fieldName);
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public byte[] getCacheKey() {
        return CacheKeyProvider.getQueryKey(this, this.fieldName);
    }

    @Override
    public String getComplexTypeName() {
        return AtomCube.TYPE_NAME;
    }

    @Override
    public ValueType getType() {
        return ValueType.COMPLEX;
    }

    @Override
    public ValueType getFinalizedType() {
        return ValueType.COMPLEX;
    }

    @Override
    public int getMaxIntermediateSize() {
        return 0;
    }

    @Override
    public String toString() {
        return "AtomCubeAggregatorFactory{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }
}