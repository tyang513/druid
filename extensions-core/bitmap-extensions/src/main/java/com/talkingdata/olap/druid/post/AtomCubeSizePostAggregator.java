package com.talkingdata.olap.druid.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableSet;
import com.talkingdata.olap.druid.BitmapUtil;
import com.talkingdata.olap.druid.cache.CacheKeyProvider;
import com.talkingdata.olap.druid.query.AtomCubeRow;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.ValueType;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

@JsonTypeName("atomCubeSize")
public class AtomCubeSizePostAggregator implements PostAggregator {
    public static final String TYPE_NAME = "atomCubeSize";

    private final String name;
    private final String field;
    private final CardinalityFunc func;
    private final int threshold;
    private final boolean persist;
    private RoaringBitmap persistBitmap;

    @JsonCreator
    public AtomCubeSizePostAggregator(
            @JsonProperty("name") String name,
            @JsonProperty("field") String field,
            @JsonProperty("func") String func,
            @JsonProperty("threshold") Integer threshold,
            @JsonProperty("persist") boolean persist) {
        this.name = name;
        this.field = field;
        this.func = CardinalityFunc.fromValue(func);
        this.threshold = threshold == null || threshold < 1 ? 10 : threshold;
        this.persist = persist;
    }

    @Override
    public Set<String> getDependentFields() {
        return ImmutableSet.of("name", "field");
    }

    @Override
    public Comparator<MutableRoaringBitmap> getComparator() {
        return BitmapUtil.MRB_COMPARATOR;
    }

    @Override
    public Object compute(final Map<String, Object> combinedAggregators) {
        List<AtomCubeRow> results = (List<AtomCubeRow>) combinedAggregators.get(field);
        if (results == null) {
            return func == CardinalityFunc.COMBINE ? 0 : Collections.emptyList();
        }

        if (func == CardinalityFunc.COMBINE) {
            List<MutableRoaringBitmap> bitmaps = results
                    .stream()
                    .map(r -> {
                        MutableRoaringBitmap bitmap = r.getBitmapValue();
                        return bitmap != null ? bitmap : BitmapUtil.EMPTY_BITMAP;
                    })
                    .collect(Collectors.toList());

            MutableRoaringBitmap unionBitmap = BitmapUtil.union(bitmaps);
            persistBitmap = unionBitmap.toRoaringBitmap();
            return unionBitmap.getCardinality();
        } else {
            results.forEach(AtomCubeRow::bitmapToCardinality);
            if (func == CardinalityFunc.DISCRETE) {
                return results;
            }

            Collections.sort(results, (AtomCubeRow o1, AtomCubeRow o2) ->
                    Integer.compare(o2.getCardinalityValue(), o1.getCardinalityValue())
            );

            int size = Math.min(results.size(), this.threshold);
            return results.subList(0, size);
        }
    }

    public RoaringBitmap getPersistBitmap() {
        return persist ? persistBitmap : null;
    }

    public void setPersistBitmap(RoaringBitmap persistBitmap) {
        this.persistBitmap = persistBitmap;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getField() {
        return field;
    }

    @JsonProperty
    public boolean isPersist() {
        return persist;
    }

    @JsonProperty
    public String getFunc() {
        return func.name();
    }

    @JsonProperty
    public Integer getThreshold() {
        return threshold;
    }

    @Override
    public String toString() {
        return "AtomCubeSizePostAggregator{" +
                "name='" + name + '\'' +
                ", field='" + field + '\'' +
                ", function=" + func +
                ", threshold=" + threshold +
                '}';
    }

    @Nullable
    @Override
    public ValueType getType() {
        return ValueType.COMPLEX;
    }

    @Override
    public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return this;
    }

    @Override
    public byte[] getCacheKey() {
        return CacheKeyProvider.getQueryKey(this, name, field, func, threshold);
    }
}
