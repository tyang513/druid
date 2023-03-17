package com.talkingdata.olap.druid.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.talkingdata.olap.druid.query.AtomCubeQuery;
import com.talkingdata.olap.druid.query.AtomCubeRow;
import java.lang.reflect.Type;
import org.apache.druid.query.CacheStrategy;

/**
 * QueryCacheStrategy
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-26
 */
public class QueryCacheStrategy implements CacheStrategy<AtomCubeRow, CacheItem<AtomCubeRow>, AtomCubeQuery> {
    private final long CACHE_EXPIRE = 60000;
    private static final TypeReference<CacheItem<AtomCubeRow>> typeReference = new TypeReference<CacheItem<AtomCubeRow>>() {
        @Override
        public Type getType() {
            return super.getType();
        }
    };

    @Override
    public boolean isCacheable(AtomCubeQuery query, boolean willMergeRunners) {
        return true;
    }

    @Override
    public byte[] computeCacheKey(AtomCubeQuery query) {
        return query.getSubKey();
    }

    @Override
    public byte[] computeResultLevelCacheKey(AtomCubeQuery query) {
        return query.getMainKey().key;
    }

    @Override
    public TypeReference<CacheItem<AtomCubeRow>> getCacheObjectClazz() {
        return typeReference;
    }

    @Override
    public Function<AtomCubeRow, CacheItem<AtomCubeRow>> prepareForCache(boolean isResultLevelCache) {
        return result -> new CacheItem(result);
    }

    @Override
    public Function<CacheItem<AtomCubeRow>, AtomCubeRow> pullFromCache(boolean isResultLevelCache) {
        return cacheItem -> {
            if ((System.currentTimeMillis() - cacheItem.getCreated()) > CACHE_EXPIRE) {
                return null;
            }

            return cacheItem.getValue();
        };
    }
}
