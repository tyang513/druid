package com.talkingdata.olap.druid.query;

import com.talkingdata.olap.druid.store.AtomCubeStore;
import com.talkingdata.olap.druid.store.DataSetOperation;
import java.util.List;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * AtomCubeQuery
 *
 * @author minfengxu
 * @version v1
 * @since 2017/4/10
 */
public class AtomCubeDataSetQueryRunner implements QueryRunner<AtomCubeRow> {
    public static final String PERSISTED_BITMAP = "persisted_bitmap";
    private final AtomCubeStore store;

    public AtomCubeDataSetQueryRunner(AtomCubeStore store) {
        this.store = store;
    }

    public Sequence<AtomCubeRow> run(QueryPlus<AtomCubeRow> queryPlus, ResponseContext responseContext) {
        Query query = queryPlus.getQuery();
        if (query instanceof AtomCubeDataSetQuery) {
            AtomCubeDataSetQuery dataSetQuery = (AtomCubeDataSetQuery) query;

            List<String> keys = dataSetQuery.getKeys();
            DataSetOperation operation = DataSetOperation.fromValue(dataSetQuery.getOperation());
            MutableRoaringBitmap cachedValue = store.get(keys, operation);

            if (cachedValue != null) {
                AtomCubeRow result = new AtomCubeRow();
                result.put(PERSISTED_BITMAP, cachedValue);
                return result.toSequence();
            }
        }
        return Sequences.empty();
    }
}
