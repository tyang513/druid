package com.talkingdata.olap.druid.query;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import com.talkingdata.olap.druid.cache.CacheItem;
import com.talkingdata.olap.druid.post.AtomCubeRawPostAggregator;
import com.talkingdata.olap.druid.post.AtomCubeSetPostAggregator;
import com.talkingdata.olap.druid.post.AtomCubeSizePostAggregator;
import com.talkingdata.olap.druid.store.AtomCubeStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.AbstractPrioritizedCallable;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.initialization.ServerConfig;
import org.roaringbitmap.RoaringBitmap;

/**
 * AtomCubeQueryRunner
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class AtomCubeQueryRunner implements QueryRunner<AtomCubeRow> {
    private static final int DEFAULT_CORE_SIZE = 1000;
    private static final String CORE_FIELD = "query.core";
    private static final String PERSIST_POSTFIX = ".persist_key";
    private static final DebugLogger TRACER = DebugLoggerFactory.getLogger(AtomCubeQueryRunner.class);
    private static final Map<String, ContinueReceiver> CONDITION_PARSERS;
    private static final ListeningExecutorService TASK_POOL;

    static {
        CONDITION_PARSERS = new HashMap<>();
        CONDITION_PARSERS.put(Query.TIMESERIES, new TimeseriesContinueReceiver());
        CONDITION_PARSERS.put(Query.GROUP_BY, new GroupByContinueReceiver());

        int coreSize = Integer.getInteger(CORE_FIELD, DEFAULT_CORE_SIZE);
        TASK_POOL = MoreExecutors.listeningDecorator(Executors.newWorkStealingPool(coreSize));
        TRACER.debug("Creating task pool", coreSize);
    }

    private final QuerySegmentWalker querySegmentWalker;
    private final ServerConfig serverConfig;
    private final Cache cache;
    private final AtomCubeStore store;
    private final CacheConfig cacheConfig;
    private final ObjectMapper objectMapper;
    private final AtomCubeQueryToolChest toolChest;
    private final BlockingDeque<Pair<String, List<AtomCubeRow>>> workQueue;
    private volatile boolean workDone = false;

    public AtomCubeQueryRunner(ObjectMapper objectMapper, ServerConfig serverConfig, Cache cache, AtomCubeStore store,
                               CacheConfig cacheConfig, QuerySegmentWalker querySegmentWalker,
                               AtomCubeQueryToolChest toolChest) {
        TRACER.debug("Initializing", this, objectMapper, serverConfig, cache, store, cacheConfig, querySegmentWalker, toolChest);

        this.objectMapper = objectMapper;
        this.serverConfig = serverConfig;
        this.cache = cache;
        this.store = store;
        this.cacheConfig = cacheConfig;
        this.toolChest = toolChest;
        this.querySegmentWalker = querySegmentWalker;
        this.workQueue = new LinkedBlockingDeque<>();
    }

    @Override
    public Sequence<AtomCubeRow> run(QueryPlus<AtomCubeRow> queryPlus, ResponseContext responseContext) {
        // 1. 校验query类型
        TRACER.debug("1. Validating query type", queryPlus);
        AtomCubeQuery mainQuery;
        Query query = queryPlus.getQuery();
        if (query instanceof AtomCubeQuery) {
            mainQuery = (AtomCubeQuery) query;
        } else {
            return Sequences.empty();
        }

        // 2. 校验是否存在PostAggregator
        List<PostAggregator> aggregators = mainQuery.getPostAggregators();
        TRACER.debug("2. Validating postAggregator", aggregators);
        if (aggregators == null || aggregators.isEmpty()) {
            return Sequences.empty();
        }

        // 3. 从缓存中获取
        CacheHelper cacheHelper = null;
        TRACER.debug("3. Pulling result from cache", cacheConfig.isUseCache());
        if (cacheConfig.isUseCache()) {
            cacheHelper = new CacheHelper(mainQuery);
            AtomCubeRow pulledResult = cacheHelper.pull();
            if (pulledResult != null) {
                return pulledResult.toSequence();
            }
        }

        // 4. 提交子任务
        ListenableFuture<List<List<AtomCubeRow>>> subTasks = Futures.allAsList(mainQuery
                .getQueries()
                .entrySet()
                .stream()
                .map(r -> {
                    int priority = (Integer) r.getValue().getContextValue(QueryContexts.PRIORITY_KEY, 0);
                    return TASK_POOL.submit(new Worker(priority, r, responseContext));
                }).collect(Collectors.toList()));
        TRACER.debug("4. Submitted tasks", subTasks);

        // 5. 线程：任务状态监控
        TASK_POOL.execute(() -> {
            boolean isCompleted = isCompleted(subTasks, mainQuery);
            TRACER.debug("5. Executed status", isCompleted);
            if (!isCompleted) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                isCompleted(subTasks, mainQuery);
            }
            workDone = true;
        });

        // 6. 当前线程：聚合结果
        Map<String, Object> aggregatedTmpResult = new HashMap<>();
        List<PostAggregator> waitAggregators = new ArrayList<>(aggregators);
        while (!workDone || !workQueue.isEmpty()) {
            try {
                Pair<String, List<AtomCubeRow>> taskResult = workQueue.poll(100, TimeUnit.MILLISECONDS);
                if (taskResult != null) {
                    TRACER.debug("6. Polled one result", taskResult);
                    aggregatedTmpResult.put(taskResult.lhs, taskResult.rhs);
                    aggregate(aggregatedTmpResult, waitAggregators);
                }
            } catch (InterruptedException e) {
                TRACER.error("Polled result error", e);
            }
        }

        if (waitAggregators.size() > 0) {
            aggregate(aggregatedTmpResult, waitAggregators);
        }

        // 7. 处理输出结果，只保留PostAggregator
        Map<String, Object> aggregatedResult = new HashMap<>();
        for (PostAggregator postAggregator : aggregators) {
            // Fix Bug：AtomCubeSetPostAggregator 不输出结果
            if (postAggregator instanceof AtomCubeSetPostAggregator) {
                continue;
            }

            String name = postAggregator.getName();
            aggregatedResult.put(name, aggregatedTmpResult.get(name));

            String storeKey = name + PERSIST_POSTFIX;
            Object storeValue = aggregatedTmpResult.get(storeKey);
            if (storeValue != null) {
                aggregatedResult.put(storeKey, storeValue);
            }
        }

        TRACER.debug("7. Aggregated result", aggregatedResult);

        AtomCubeRow row = new AtomCubeRow(aggregatedResult);
        if (cacheHelper != null) {
            TRACER.debug("8. Caching result", row);
            cacheHelper.prepare(row);
        }

        return row.toSequence();
    }

    private boolean isCompleted(ListenableFuture<List<List<AtomCubeRow>>> futures, AtomCubeQuery mainQuery) {
        try {
            Object timeout = mainQuery.getContextValue(QueryContexts.TIMEOUT_KEY);
            List result = timeout instanceof Number ? futures.get(((Number) timeout).longValue(), TimeUnit.MILLISECONDS) : futures.get();
            return result != null && mainQuery.getQueries().size() == result.size();
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            futures.cancel(true);
            throw new QueryInterruptedException(e);
        } catch (TimeoutException e) {
            futures.cancel(true);
            throw new QueryInterruptedException(e);
        }
    }

    private void aggregate(Map<String, Object> results, List<PostAggregator> aggregators) {
        Iterator<PostAggregator> iterator = aggregators.iterator();
        while (iterator.hasNext()) {
            PostAggregator postAggregator = iterator.next();
            if (isAggregated(results, postAggregator)) {
                String aggregatorName = postAggregator.getName();
                results.put(aggregatorName, postAggregator.compute(results));
                String key = store(postAggregator);
                if (StringUtils.isNotEmpty(key)) {
                    results.put(aggregatorName + PERSIST_POSTFIX, key);
                }
                iterator.remove();
            }
        }
    }

    private boolean isAggregated(Map<String, Object> results, PostAggregator aggregator) {
        String field = null;
        if (aggregator instanceof AtomCubeSetPostAggregator) {
            List<String> fields = ((AtomCubeSetPostAggregator) aggregator).getFields();
            int size = fields.stream().mapToInt(r -> results.get(r) != null ? 1 : 0).sum();
            return size == fields.size();
        } else if (aggregator instanceof AtomCubeSizePostAggregator) {
            field = ((AtomCubeSizePostAggregator) aggregator).getField();
        } else if (aggregator instanceof AtomCubeRawPostAggregator) {
            field = ((AtomCubeRawPostAggregator) aggregator).getField();
        }
        return StringUtils.isNotEmpty(field) && results.get(field) != null;
    }

    private String store(PostAggregator postAggregator) {
        RoaringBitmap bitmap = null;
        if (postAggregator instanceof AtomCubeSetPostAggregator) {
            bitmap = ((AtomCubeSetPostAggregator) postAggregator).getPersistBitmap();
        } else if (postAggregator instanceof AtomCubeRawPostAggregator) {
            bitmap = ((AtomCubeRawPostAggregator) postAggregator).getPersistBitmap();
        } else if (postAggregator instanceof AtomCubeSizePostAggregator) {
            bitmap = ((AtomCubeSizePostAggregator) postAggregator).getPersistBitmap();
        }

        if (bitmap == null) {
            return null;
        }

        String key = UUID.randomUUID().toString();
        store.put(key, bitmap);
        return key;
    }

    private class Worker extends AbstractPrioritizedCallable<List<AtomCubeRow>> {
        private final Map.Entry<String, Query> entry;
        private final ResponseContext responseContext;

        public Worker(int priority, Map.Entry<String, Query> entry, ResponseContext responseContext) {
            super(priority);

            this.entry = entry;
            this.responseContext = responseContext;
        }

        @Override
        public List<AtomCubeRow> call() throws Exception {
            Query query = entry.getValue();
            Yielder yielder = execute(query, responseContext);

            if (yielder == null) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    TRACER.error("Queried error", e);
                }
                yielder = execute(query, responseContext);
                if (yielder == null) {
                    return null;
                }
            }

            List<AtomCubeRow> value = CONDITION_PARSERS.get(query.getType()).receive(query, yielder);
            workQueue.offer(Pair.of(entry.getKey(), value));
            return value;
        }

        private Yielder execute(Query query, ResponseContext responseContext) {
            // 1. 设置queryId
            String queryId = query.getId();
            if (queryId == null) {
                queryId = UUID.randomUUID().toString();
                query = query.withId(queryId);
            }

            // 2. 设置查询超时时间
            Long timeout = (Long) query.getContextValue(QueryContexts.TIMEOUT_KEY);
            if (timeout == null) {
                timeout = serverConfig.getMaxIdleTime().toStandardDuration().getMillis();
                ImmutableMap<String, Long> map = ImmutableMap.of(QueryContexts.TIMEOUT_KEY, timeout);
                query = query.withOverriddenContext(map);
            }

            // 3. 执行结果
            ResponseContext context = ResponseContext.createEmpty();
            context.merge(responseContext);
            context.put(ResponseContext.Key.QUERY_FAIL_DEADLINE_MILLIS, System.currentTimeMillis() + timeout);

            Sequence result = query.getRunner(querySegmentWalker).run(QueryPlus.wrap(query), context);
            if (result == null) {
                result = Sequences.empty();
            }

            return result.toYielder(null, new YieldingAccumulator() {
                @Override
                public Object accumulate(Object accumulated, Object in) {
                    yield();
                    return in;
                }
            });
        }


    }

    private class CacheHelper {
        private final Cache.NamedKey mainKey;
        private final CacheStrategy<AtomCubeRow, CacheItem<AtomCubeRow>, AtomCubeQuery> cacheStrategy;

        public CacheHelper(AtomCubeQuery mainQuery) {
            this.mainKey = mainQuery.getMainKey();
            this.cacheStrategy = toolChest.getCacheStrategy(mainQuery);
        }

        public AtomCubeRow pull() {
            // 1. 校验是否存在缓存中
            byte[] value = cache.get(mainKey);
            if (value == null || value.length == 0) {
                return null;
            }

            try {
                // 2. 解析缓存进行反序列化操作
                JsonParser jsonParser = objectMapper.getFactory().createParser(value);
                CacheItem<AtomCubeRow> cacheItem = objectMapper.readValue(jsonParser, cacheStrategy.getCacheObjectClazz());
                AtomCubeRow row = cacheStrategy.pullFromCache(true).apply(cacheItem);
                if (row != null) {
                    return row;
                }
            } catch (IOException e) {
                TRACER.error("Pull from cache error", e);
            }

            // 3. 缓存无效
            cache.put(mainKey, new byte[0]);
            return null;
        }

        public void prepare(AtomCubeRow result) {
            try {
                Object cachedValue = cacheStrategy.prepareForCache(true).apply(result);
                byte[] valueBytes = objectMapper.writeValueAsBytes(cachedValue);
                cache.put(mainKey, valueBytes);
            } catch (Exception e) {
                TRACER.error("Prepared to cache error", e);
            }
        }
    }
}