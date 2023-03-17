package com.talkingdata.olap.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import com.talkingdata.olap.druid.cache.CacheKeyProvider;
import com.talkingdata.olap.druid.serde.AtomCube;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;

/**
 * AtomCubeQuery
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
@JsonTypeName(AtomCube.TYPE_NAME)
public class AtomCubeQuery extends BaseQuery<AtomCubeRow> {
    private static final DebugLogger TRACER = DebugLoggerFactory.getLogger(AtomCubeQuery.class);

    private final Map<String, Query> queries;
    private final List<PostAggregator> postAggregators;
    private final Map<String, Cache.NamedKey> subKeys;
    private final int subSize;
    private final byte[] subKey;
    private final Cache.NamedKey mainKey;

    @JsonProperty("queries")
    public Map<String, Query> getQueries() {
        return queries;
    }

    @JsonProperty("postAggregations")
    public List<PostAggregator> getPostAggregators() {
        return postAggregators;
    }

    public Cache.NamedKey getMainKey() {
        return this.mainKey;
    }

    public Map<String, Cache.NamedKey> getSubKeys() {
        return this.subKeys;
    }

    public byte[] getSubKey() {
        return subKey;
    }

    public int getSubSize() {
        return subSize;
    }

    @JsonCreator
    public AtomCubeQuery(
            @JsonProperty("dataSource") DataSource dataSource,
            @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
            @JsonProperty("queries") Map<String, Query> queries,
            @JsonProperty("postAggregations") List<PostAggregator> postAggregators,
            @JsonProperty("context") Map<String, Object> context) {
        super(dataSource, querySegmentSpec, false, context);

        this.queries = queries;
        this.postAggregators = postAggregators;

        // 1. 子查询key
        this.subSize = queries.size();
        this.subKeys = getSubKeys(queries);
        // 2. 子查询组合key
        this.subKey = getSubKey(this.subKeys);
        // 3. 主查询key
        this.mainKey = getMainKey(this.subKeys, postAggregators);

        TRACER.debug("Initializing", this, this.queries, this.postAggregators, this.subKeys, this.mainKey);
    }

    private TreeMap<String, Cache.NamedKey> getSubKeys(Map<String, Query> queries) {
        TreeMap<String, Cache.NamedKey> subKeys = new TreeMap<>();
        for (Map.Entry<String, Query> entry : queries.entrySet()) {
            subKeys.put(entry.getKey(), CacheKeyProvider.getQueryKey(entry.getValue()));
        }
        return subKeys;
    }

    private byte[] getSubKey(Map<String, Cache.NamedKey> subKeys) {
        int size = subKeys.values().stream().mapToInt(r -> r.key.length).sum();

        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        for (Map.Entry<String, Cache.NamedKey> entry : subKeys.entrySet()) {
            byteBuffer.put(entry.getValue().key);
        }

        return byteBuffer.array();
    }

    private Cache.NamedKey getMainKey(Map<String, Cache.NamedKey> subQueryKeys, List<PostAggregator> postAggregators) {
        Map<String, String> postAggregatorKeys = new TreeMap<>();
        for (Map.Entry<String, Cache.NamedKey> entry : subQueryKeys.entrySet()) {
            postAggregatorKeys.put(entry.getKey(), entry.getValue().namespace);
        }

        // 1. 查询namespace
        String namespace = StringUtils.join(postAggregatorKeys.values());
        namespace = Integer.toHexString(namespace.hashCode());

        // 2. 统计postAggregatorKeys
        if (postAggregators != null) {
            for (PostAggregator postAggregator : postAggregators) {
                Cache.NamedKey namedKey = CacheKeyProvider.getPostKey(subQueryKeys, postAggregator);
                postAggregatorKeys.put(postAggregator.getName(), namedKey.namespace);
            }
        }

        // 3. 生成key
        byte[] bytes = StringUtils.join(postAggregatorKeys.values()).getBytes();
        return new Cache.NamedKey(namespace, bytes);
    }

    // # region 重写方法
    @Override
    public AtomCubeQuery withQuerySegmentSpec(QuerySegmentSpec spec) {
        return new AtomCubeQuery(
                getDataSource(),
                spec,
                queries,
                postAggregators,
                getContext());
    }

    @Override
    public AtomCubeQuery withDataSource(DataSource dataSource) {
        return new AtomCubeQuery(
                dataSource,
                getQuerySegmentSpec(),
                queries,
                postAggregators,
                getContext());
    }

    @Override
    public AtomCubeQuery withOverriddenContext(Map contextOverrides) {
        return new AtomCubeQuery(
                getDataSource(),
                getQuerySegmentSpec(),
                queries,
                postAggregators,
                computeOverridenContext(contextOverrides)
        );
    }

    @Override
    public String getType() {
        return AtomCube.TYPE_NAME;
    }

    @Override
    public boolean hasFilters() {
        return false;
    }

    @Override
    public DimFilter getFilter() {
        return null;
    }

    @Override
    public String toString() {
        return new StringBuilder(2048)
                .append("AtomCubeQuery {\n")
                .append("subQueries [\n")
                .append(queries
                        .entrySet()
                        .stream()
                        .map(Map.Entry::toString)
                        .collect(Collectors.joining(",")))
                .append("]\n")
                .append("postAggregators [")
                .append(postAggregators
                        .stream()
                        .map(PostAggregator::toString)
                        .collect(Collectors.joining(",")))
                .append("]\n}")
                .toString();
    }

    // # endregion
}
