package com.talkingdata.olap.druid.query;

import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import com.talkingdata.olap.druid.serde.AtomCube;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * ParentContinueReceiver
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-09
 */
public abstract class BaseContinueReceiver implements ContinueReceiver<Query> {
    protected DebugLogger tracer;

    protected BaseContinueReceiver() {
        tracer = DebugLoggerFactory.getLogger(this.getClass());
    }

    @Override
    public List<AtomCubeRow> receive(Query query, Yielder yielder) {
        String metricName;
        if (yielder == null || StringUtils.isEmpty((metricName = getAtomCubeMetric(query)))) {
            return null;
        }

        List<AtomCubeRow> result = new ArrayList<>();
        while (!yielder.isDone()) {
            Object o = yielder.get();
            List<AtomCubeRow> list = receive(metricName, query, o);
            result.addAll(list);
            yielder = yielder.next(null);
        }
        try {
            yielder.close();
        } catch (IOException e) {
            tracer.error("Closed error", e);
        }
        return result;
    }

    protected abstract String getAtomCubeMetric(Query query);

    protected abstract List<AtomCubeRow> receive(String metric, Query query, Object input);

    protected String getAtomCubeMetric(List<AggregatorFactory> list) {
        if (list != null) {
            for (AggregatorFactory factory : list) {
                if (AtomCube.TYPE_NAME.equals(factory.getComplexTypeName())) {
                    return factory.getName();
                }
            }
        }
        return null;
    }

    protected MutableRoaringBitmap toMutableRoaringBitmap(Object obj) {
        return obj instanceof RoaringBitmap ? ((RoaringBitmap) obj).toMutableRoaringBitmap() : null;
    }
}
