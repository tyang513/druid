package com.talkingdata.olap.druid.cache;

import com.google.common.collect.Ordering;
import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import com.talkingdata.olap.druid.post.AtomCubeRawPostAggregator;
import com.talkingdata.olap.druid.post.AtomCubeSetPostAggregator;
import com.talkingdata.olap.druid.post.AtomCubeSizePostAggregator;
import com.talkingdata.olap.druid.serde.AtomCubeAggregatorFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.joda.time.Interval;

/**
 * CacheProvider
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class CacheKeyProvider {
    private static final DebugLogger TRACER = DebugLoggerFactory.getLogger(CacheKeyProvider.class);
    private static final Map<Class<?>, byte[]> PREFIX_MAP;

    static {
        PREFIX_MAP = new HashMap<>();
        PREFIX_MAP.put(AtomCubeAggregatorFactory.class, new byte[]{(byte) 0xFF, (byte) 0x01});
        PREFIX_MAP.put(AtomCubeRawPostAggregator.class, new byte[]{(byte) 0xFF, (byte) 0x02});
        PREFIX_MAP.put(AtomCubeSetPostAggregator.class, new byte[]{(byte) 0xFF, (byte) 0x03});
        PREFIX_MAP.put(AtomCubeSizePostAggregator.class, new byte[]{(byte) 0xFF, (byte) 0x04});
    }

    public static Cache.NamedKey getQueryKey(Query<?> query) {
        // 1. 表名key
        String tableKey = query
                .getDataSource()
                .getTableNames()
                .stream()
                .sorted(Ordering.natural())
                .collect(Collectors.joining());

        TRACER.debug("Built tableKey", tableKey);

        // 2. 区间key
        String intervalKey = query
                .getIntervals()
                .stream()
                .sorted(Comparator.nullsFirst(Comparator.comparing(Interval::getStartMillis).reversed()))
                .map(r -> r.getStartMillis() + "" + r.getEndMillis())
                .collect(Collectors.joining());

        TRACER.debug("Built intervalKey", intervalKey);

        // 3. 计算queryId
        String queryId = Integer.toHexString((tableKey + intervalKey).hashCode());
        TRACER.debug("Built queryId", queryId);
        byte[] queryIdBytes = queryId.getBytes();

        // 4. 过滤条件key
        byte[] filterKey = getFilterKey(query);
        TRACER.debug("Built filterKey", () -> Arrays.toString(filterKey));

        // 5. 计算缓存结果
        return new Cache.NamedKey(queryId,
                ByteBuffer.allocate(queryIdBytes.length + filterKey.length)
                        .put(queryIdBytes)
                        .put(filterKey)
                        .array());
    }

    private static byte[] getFilterKey(Query<?> query) {
        if (query.hasFilters()) {
            if (query instanceof TimeseriesQuery) {
                return ((TimeseriesQuery) query).getDimensionsFilter().getCacheKey();
            } else if (query instanceof TopNQuery) {
                return ((TopNQuery) query).getDimensionsFilter().getCacheKey();
            } else if (query instanceof GroupByQuery) {
                return ((GroupByQuery) query).getDimFilter().getCacheKey();
            }
        }
        return new byte[0];
    }

    public static byte[] getQueryKey(Object instance, Object... properties) {
        byte[] prefix = PREFIX_MAP.get(instance.getClass());
        return prefix == null ? new byte[0] : getQueryKey(prefix, properties);
    }

    private static byte[] getQueryKey(byte[] prefix, Object... properties) {
        byte[] bytes = StringUtils.toUtf8WithNullToEmpty(join(properties));
        return ByteBuffer.allocate(2 + bytes.length)
                .put(prefix)
                .put(bytes)
                .array();
    }

    private static String join(Object... properties) {
        if (properties == null || properties.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder(properties.length);
        for (Object property : properties) {
            if (property instanceof Collection) {
                Collection collection = (Collection) property;
                for (Object item : collection) {
                    sb.append(item != null ? item.toString() : "");
                }
            } else {
                sb.append(property != null ? property.toString() : "");
            }
        }
        return sb.toString();
    }


    public static Cache.NamedKey getPostKey(Map<String, Cache.NamedKey> postKeys, PostAggregator postAggregator) {
        Cache.NamedKey key = new Cache.NamedKey("", new byte[0]);
        if (postAggregator instanceof AtomCubeRawPostAggregator) {
            key = getPostKey(postKeys, (AtomCubeRawPostAggregator) postAggregator);
        } else if (postAggregator instanceof AtomCubeSetPostAggregator) {
            key = getPostKey(postKeys, (AtomCubeSetPostAggregator) postAggregator);
        } else if (postAggregator instanceof AtomCubeSizePostAggregator) {
            key = getPostKey(postKeys, (AtomCubeSizePostAggregator) postAggregator);
        }
        postKeys.put(postAggregator.getName(), key);
        return key;
    }

    public static Cache.NamedKey getPostKey(Map<String, Cache.NamedKey> postKeys,
                                            AtomCubeRawPostAggregator postAggregator) {
        return getPostKey(
                postKeys,
                postAggregator.getName(),
                Collections.singletonList(postAggregator.getField()),
                postAggregator.getFormat().toString());
    }

    private static Cache.NamedKey getPostKey(Map<String, Cache.NamedKey> postKeys, String name, List<String> fields,
                                             String... properties) {
        String key = new StringBuilder(256)
                .append(Arrays.stream(properties).collect(Collectors.joining()))
                .append(fields
                        .stream()
                        .map(r -> Integer.toHexString(postKeys.get(r).hashCode()))
                        .collect(Collectors.joining()))
                .append(name)
                .toString();
        key = Integer.toHexString(key.hashCode());
        return new Cache.NamedKey(key, key.getBytes());
    }

    private static Cache.NamedKey getPostKey(Map<String, Cache.NamedKey> postKeys,
                                             AtomCubeSetPostAggregator postAggregator) {
        return getPostKey(
                postKeys,
                postAggregator.getName(),
                postAggregator.getFields(),
                postAggregator.getFunc());
    }

    private static Cache.NamedKey getPostKey(Map<String, Cache.NamedKey> postKeys,
                                             AtomCubeSizePostAggregator postAggregator) {
        return getPostKey(
                postKeys,
                postAggregator.getName(),
                Collections.singletonList(postAggregator.getField()),
                "cardinality");
    }
}


