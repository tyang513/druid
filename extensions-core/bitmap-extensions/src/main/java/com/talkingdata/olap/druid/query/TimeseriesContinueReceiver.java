package com.talkingdata.olap.druid.query;

import java.util.ArrayList;
import java.util.List;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * TimeseriesContinueReceiver
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-09
 */
public class TimeseriesContinueReceiver extends BaseContinueReceiver {
    @Override
    protected String getAtomCubeMetric(Query query) {
        return getAtomCubeMetric(((TimeseriesQuery) query).getAggregatorSpecs());
    }

    @Override
    protected List<AtomCubeRow> receive(String metric, Query query, Object input) {
        List<AtomCubeRow> list = new ArrayList<>();
        if (input instanceof Result) {
            Result<TimeseriesResultValue> result = (Result<TimeseriesResultValue>) input;
            String timestamp = result.getTimestamp().toString();
            MutableRoaringBitmap bitmap = toMutableRoaringBitmap(result.getValue().getMetric(metric));
            list.add(new AtomCubeRow(timestamp, bitmap));
        }
        return list;
    }
}
