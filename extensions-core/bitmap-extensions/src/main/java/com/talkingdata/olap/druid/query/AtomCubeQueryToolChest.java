package com.talkingdata.olap.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import com.talkingdata.olap.druid.cache.CacheItem;
import com.talkingdata.olap.druid.cache.QueryCacheStrategy;
import com.talkingdata.olap.druid.serde.AtomCube;
import com.talkingdata.olap.druid.store.AtomCubeStore;
import javax.annotation.Nullable;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.server.initialization.ServerConfig;

/**
 * AtomCubeQueryToolChest
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class AtomCubeQueryToolChest extends QueryToolChest<AtomCubeRow, AtomCubeQuery> {
    private static final DebugLogger TRACER = DebugLoggerFactory.getLogger(AtomCubeQueryToolChest.class);
    private static final TypeReference<AtomCubeRow> TYPE =
            new TypeReference<AtomCubeRow>() {
            };

    private final ObjectMapper objectMapper;
    private final ServerConfig serverConfig;
    private final Cache cache;
    private final AtomCubeStore store;
    private final CacheConfig cacheConfig;
    private final Provider<QuerySegmentWalker> walkerProvider;

    @Inject
    public AtomCubeQueryToolChest(
            ObjectMapper objectMapper,
            ServerConfig serverConfig,
            Cache cache,
            AtomCubeStore store,
            CacheConfig cacheConfig,
            @Named(AtomCube.TYPE_NAME) Provider<QuerySegmentWalker> walkerProvider) {

        this.objectMapper = objectMapper;
        this.serverConfig = serverConfig;
        this.cache = cache;
        this.store = store;
        this.cacheConfig = cacheConfig;
        this.walkerProvider = walkerProvider;
    }

    @Override
    public QueryRunner<AtomCubeRow> mergeResults(QueryRunner<AtomCubeRow> runner) {
        return new AtomCubeQueryRunner(objectMapper, serverConfig, cache, store, cacheConfig, walkerProvider.get(), this);
    }

    @Override
    public QueryMetrics<? super AtomCubeQuery> makeMetrics(AtomCubeQuery query) {
        return new DefaultQueryMetrics();
    }

    @Override
    public TypeReference<AtomCubeRow> getResultTypeReference() {
        return TYPE;
    }

    @Override
    public Function<AtomCubeRow, AtomCubeRow> makePostComputeManipulatorFn(AtomCubeQuery query,
                                                                           MetricManipulationFn fn) {
        return Functions.identity();
    }

    @Override
    public Function<AtomCubeRow, AtomCubeRow> makePreComputeManipulatorFn(AtomCubeQuery query,
                                                                          MetricManipulationFn fn) {
        return Functions.identity();
    }

    @Nullable
    @Override
    public CacheStrategy<AtomCubeRow, CacheItem<AtomCubeRow>, AtomCubeQuery> getCacheStrategy(AtomCubeQuery query) {
        return new QueryCacheStrategy();
    }
}
