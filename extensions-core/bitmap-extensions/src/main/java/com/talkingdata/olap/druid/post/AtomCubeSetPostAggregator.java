package com.talkingdata.olap.druid.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableSet;
import com.talkingdata.olap.druid.BitmapUtil;
import com.talkingdata.olap.druid.cache.CacheKeyProvider;
import com.talkingdata.olap.druid.query.AtomCubeRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.ValueType;
import org.joda.time.DateTime;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

@JsonTypeName("atomCubeSet")
public class AtomCubeSetPostAggregator implements PostAggregator {
    public static final String TYPE_NAME = "atomCubeSet";
    private static final long MILLS_19700101 = new DateTime("1970-01-01").getMillis();
    private static final Map<DataSetFunc, Function<List<MutableRoaringBitmap>, MutableRoaringBitmap>> SIMPLE_FUNCTION_MAP;
    private static final Map<DataSetFunc, BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, MutableRoaringBitmap>> COMPLEX_FUNCTION_MAP;

    static {
        SIMPLE_FUNCTION_MAP = new HashMap<>();
        SIMPLE_FUNCTION_MAP.put(DataSetFunc.UNION, BitmapUtil::union);
        SIMPLE_FUNCTION_MAP.put(DataSetFunc.INTERSECT, BitmapUtil::and);
        SIMPLE_FUNCTION_MAP.put(DataSetFunc.NOT, BitmapUtil::andNot);

        COMPLEX_FUNCTION_MAP = new HashMap<>();
        COMPLEX_FUNCTION_MAP.put(DataSetFunc.FULL_UNION, BitmapUtil::union);
        COMPLEX_FUNCTION_MAP.put(DataSetFunc.FULL_INTERSECT, BitmapUtil::and);
        COMPLEX_FUNCTION_MAP.put(DataSetFunc.FULL_NOT, BitmapUtil::andNot);
    }

    private final String name;
    private final List<String> fields;
    private final DataSetFunc func;
    private final boolean persist;
    private final Map<String, List<String>> joinMap;
    private MutableRoaringBitmap bitmap;

    @JsonCreator
    public AtomCubeSetPostAggregator(
            @JsonProperty("name") String name,
            @JsonProperty("func") String func,
            @JsonProperty("fields") List<String> fields,
            @JsonProperty("persist") boolean persist,
            @JsonProperty("join") Map<String, List<String>> join) {
        if (fields == null || fields.size() < 2) {
            throw new IAE("Illegal number of fields[%s], must be > 1", fields.size());
        }

        this.name = name;
        this.fields = fields;
        this.func = DataSetFunc.fromValue(func);
        this.persist = persist;
        this.joinMap = join == null ? Collections.emptyMap() : join;
    }


    @Override
    public Set<String> getDependentFields() {
        return ImmutableSet.of("name", "func", "fields");
    }

    @Override
    public Comparator<MutableRoaringBitmap> getComparator() {
        return BitmapUtil.MRB_COMPARATOR;
    }

    @Override
    public Object compute(final Map<String, Object> combinedAggregators) {
        Map<String, List<AtomCubeRow>> rowMap = getRowMap(combinedAggregators, fields);
        if (func == DataSetFunc.UNION
                || func == DataSetFunc.INTERSECT
                || func == DataSetFunc.NOT) {
            bitmap = simpleCompute(rowMap, SIMPLE_FUNCTION_MAP.get(func));
            AtomCubeRow atomCubeRow = new AtomCubeRow();
            atomCubeRow.setValue(bitmap);
            return Collections.singletonList(atomCubeRow);
        }
        return complexCompute(rowMap, fields, joinMap, func == DataSetFunc.FULL_UNION, COMPLEX_FUNCTION_MAP.get(func));
    }

    private Map<String, List<AtomCubeRow>> getRowMap(Map<String, Object> combinedAggregators, List<String> fields) {
        Map<String, List<AtomCubeRow>> map = new LinkedHashMap<>(fields.size(), 1);
        for (String field : fields) {
            List<AtomCubeRow> rows = (List<AtomCubeRow>) combinedAggregators.getOrDefault(field, Collections.emptyList());
            map.put(field, rows);
        }
        return map;
    }

    private MutableRoaringBitmap simpleCompute(
            Map<String, List<AtomCubeRow>> rowMap,
            Function<List<MutableRoaringBitmap>, MutableRoaringBitmap> function) {
        List<MutableRoaringBitmap> list = new ArrayList<>();
        for (Map.Entry<String, List<AtomCubeRow>> entry : rowMap.entrySet()) {
            List<MutableRoaringBitmap> bitmaps = entry.getValue()
                    .stream()
                    .map(r -> r.getBitmapValue())
                    .collect(Collectors.toList());
            list.add(BitmapUtil.union(bitmaps));
        }

        return function.apply(list);
    }

    private List<AtomCubeRow> complexCompute(
            Map<String, List<AtomCubeRow>> rowMap,
            List<String> fields,
            Map<String, List<String>> joinMap,
            boolean isUnion,
            BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, MutableRoaringBitmap> function) {
        List<AtomCubeRow> joinedRows = null;
        List<String> joinedKeys = null;
        boolean first = true;
        for (String filed : fields) {
            List<AtomCubeRow> currentRows = rowMap.get(filed);
            List<String> currentKeys = joinMap.get(filed);

            if (first) {
                first = false;
                joinedRows = currentRows;
                joinedKeys = currentKeys;
            } else {
                joinedRows = join(joinedRows, currentRows, joinedKeys, currentKeys, isUnion, function);
                joinedKeys = currentKeys;
            }
        }

        joinedRows.forEach(AtomCubeRow::bitmapToCardinality);
        return joinedRows;
    }

    private List<AtomCubeRow> join(
            List<AtomCubeRow> leftRows,
            List<AtomCubeRow> rightRows,
            List<String> leftJoin,
            List<String> rightJoin,
            boolean isUnion,
            BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, MutableRoaringBitmap> function) {
        if (isTimeFixed(leftRows)) {
            join(rightRows, leftRows.get(0), rightJoin, leftJoin, true, function);
            return rightRows;
        } else if (isTimeFixed(rightRows)) {
            join(leftRows, rightRows.get(0), leftJoin, rightJoin, true, function);
            return leftRows;
        } else {
            if (isUnion) {
                for (AtomCubeRow row : rightRows) {
                    boolean isJoined = join(leftRows, row, leftJoin, rightJoin, false, function);
                    if (!isJoined) {
                        leftRows.add(row);
                    }
                }
            } else {
                for (AtomCubeRow row : rightRows) {
                    boolean isJoined = join(leftRows, row, leftJoin, rightJoin, false, function);
                    if (!isJoined) {
                        return Collections.emptyList();
                    }
                }
            }
            return leftRows;
        }
    }

    private boolean isTimeFixed(List<AtomCubeRow> results) {
        if (results == null || results.size() != 1) {
            return false;
        }

        return MILLS_19700101 >= new DateTime(results.get(0).getTimestamp()).getMillis();
    }

    private boolean join(
            List<AtomCubeRow> leftRows,
            AtomCubeRow rightRow,
            List<String> leftJoin,
            List<String> rightJoin,
            boolean isTimeFixed,
            BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, MutableRoaringBitmap> function) {
        boolean joined = false;
        for (AtomCubeRow leftRow : leftRows) {
            // 1. 首先比较时间
            boolean timeFilterResult = isTimeFixed || leftRow.getTimestamp().equals(rightRow.getTimestamp());
            if (!timeFilterResult) {
                continue;
            }

            // 2. 比较join
            int minLength = Math.min(leftJoin == null ? 0 : leftJoin.size(), rightJoin == null ? 0 : rightJoin.size());
            boolean matched = true;
            if (minLength > 0) {
                for (int i = 0; i < minLength; i++) {
                    // 3. 获取左侧关联数据
                    String leftKey = leftJoin.get(i);
                    Object leftValue = leftRow.get(leftKey);
                    if (leftValue == null) {
                        matched = false;
                        break;
                    }

                    // 4. 获取右侧关联数据
                    String rightKey = rightJoin.get(i);
                    Object rightValue = rightRow.get(rightKey);
                    if (rightValue == null) {
                        matched = false;
                        break;
                    }

                    // 5. 比较值是否相等
                    if (!leftValue.equals(rightValue)) {
                        matched = false;
                        break;
                    }
                }
            }

            if (matched) {
                // 3. bitmap操作
                MutableRoaringBitmap leftBitmap = leftRow.getBitmapValue();
                MutableRoaringBitmap rightBitmap = rightRow.getBitmapValue();
                MutableRoaringBitmap joinResult = function.apply(leftBitmap, rightBitmap);

                rightRow.removeValue();
                leftRow.putAll(rightRow);
                leftRow.setValue(joinResult);
                joined = true;
            }
        }

        return joined;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFunc() {
        return func.toString();
    }

    @JsonProperty
    public List<String> getFields() {
        return fields;
    }

    public RoaringBitmap getPersistBitmap() {
        return persist ? bitmap.toRoaringBitmap() : null;
    }

    @JsonProperty
    public boolean isPersist() {
        return persist;
    }

    @JsonProperty
    public Map<String, List<String>> getJoinMap() {
        return joinMap;
    }

    @Nullable
    @Override
    public ValueType getType() {
        return ValueType.LONG;
    }

    @Override
    public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return this;
    }

    @Override
    public byte[] getCacheKey() {
        return CacheKeyProvider.getQueryKey(this, name, func, fields, persist, joinMap);
    }

    @Override
    public String toString() {
        return "AtomCubeSetPostAggregator{" +
                "name='" + name + '\'' +
                ", fields=" + fields +
                ", func=" + func +
                ", persist=" + persist +
                ", joinMap=" + joinMap +
                ", bitmap=" + bitmap +
                '}';
    }
}