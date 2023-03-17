package com.talkingdata.olap.druid.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableSet;
import com.talkingdata.olap.druid.BitmapUtil;
import com.talkingdata.olap.druid.cache.CacheKeyProvider;
import com.talkingdata.olap.druid.query.AtomCubeRow;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.ValueType;
import org.roaringbitmap.RoaringBitmap;

@JsonTypeName("atomCubeRaw")
public class AtomCubeRawPostAggregator implements PostAggregator {
    public static final String TYPE_NAME = "atomCubeRaw";
    private final String name;
    private final String field;
    private final RawFormat format;
    private final boolean persist;
    private RoaringBitmap persistBitmap;

    @JsonCreator
    public AtomCubeRawPostAggregator(
            @JsonProperty("name") String name,
            @JsonProperty("field") String field,
            @JsonProperty("format") String format,
            @JsonProperty("persist") boolean persist) {
        this.name = name;
        this.field = field;
        this.format = RawFormat.fromValue(format);
        this.persist = persist;
    }

    @Override
    public Set<String> getDependentFields() {
        return ImmutableSet.of("name", "field");
    }

    @Override
    public Comparator<RoaringBitmap> getComparator() {
        return BitmapUtil.COMPARATOR;
    }

    @Override
    public Object compute(final Map<String, Object> combinedAggregators) {
        List<AtomCubeRow> rows = (List<AtomCubeRow>) combinedAggregators.get(field);
        if (rows != null && rows.size() > 0) {
            if (format == null) {
                for (AtomCubeRow row : rows) {
                    persistBitmap = row.getBitmapValue().toRoaringBitmap();
                    break;
                }
                return null;
            } else if (format == RawFormat.LIST) {
                for (AtomCubeRow row : rows) {
                    RoaringBitmap bitmap = row.getBitmapValue().toRoaringBitmap();
                    row.setValue(bitmap.toArray());
                    if (persistBitmap == null) {
                        persistBitmap = bitmap;
                    }
                }
            } else {
                for (AtomCubeRow row : rows) {
                    RoaringBitmap bitmap = row.getBitmapValue().toRoaringBitmap();
                    row.setValue(BitmapUtil.toBase64(bitmap));
                    if (persistBitmap == null) {
                        persistBitmap = bitmap;
                    }
                }
            }
        }
        return rows;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public RawFormat getFormat() {
        return format;
    }

    @JsonProperty
    public String getField() {
        return field;
    }

    @JsonProperty
    public boolean isPersist() {
        return persist;
    }

    public RoaringBitmap getPersistBitmap() {
        return persist ? persistBitmap : null;
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
        return CacheKeyProvider.getQueryKey(this, name, field, format, persist);
    }

    @Override
    public String toString() {
        return "AtomCubeRawPostAggregator{" +
                "name='" + name + '\'' +
                ", field='" + field + '\'' +
                ", format=" + format +
                ", persist=" + persist +
                ", persistBitmap=" + persistBitmap +
                '}';
    }
}
