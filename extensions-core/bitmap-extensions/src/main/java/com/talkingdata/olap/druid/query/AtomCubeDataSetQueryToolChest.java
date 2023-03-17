package com.talkingdata.olap.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.inject.Inject;
import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import com.talkingdata.olap.druid.store.AtomCubeStore;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.MetricManipulationFn;

/**
 * AtomCubeDataSetQueryToolChest
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class AtomCubeDataSetQueryToolChest extends QueryToolChest<AtomCubeRow, AtomCubeDataSetQuery> {
    private static final DebugLogger TRACER = DebugLoggerFactory.getLogger(AtomCubeDataSetQueryToolChest.class);
    private static final TypeReference<AtomCubeRow> TYPE =
            new TypeReference<AtomCubeRow>() {
            };

    private final AtomCubeStore store;

    @Inject
    public AtomCubeDataSetQueryToolChest(AtomCubeStore store) {
        TRACER.debug("Initializing", this, store);
        this.store = store;
    }

    @Override
    public QueryRunner<AtomCubeRow> mergeResults(QueryRunner<AtomCubeRow> runner) {
        return new AtomCubeDataSetQueryRunner(store);
    }

    @Override
    public QueryMetrics<? super AtomCubeDataSetQuery> makeMetrics(AtomCubeDataSetQuery query) {
        return new DefaultQueryMetrics();
    }

    @Override
    public TypeReference<AtomCubeRow> getResultTypeReference() {
        return TYPE;
    }

    @Override
    public Function<AtomCubeRow, AtomCubeRow> makePostComputeManipulatorFn(AtomCubeDataSetQuery query,
                                                                           MetricManipulationFn fn) {
        return Functions.identity();
    }

    @Override
    public Function<AtomCubeRow, AtomCubeRow> makePreComputeManipulatorFn(AtomCubeDataSetQuery query,
                                                                          MetricManipulationFn fn) {
        return Functions.identity();
    }
}
