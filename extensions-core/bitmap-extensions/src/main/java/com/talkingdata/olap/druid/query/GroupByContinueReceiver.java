package com.talkingdata.olap.druid.query;

import java.util.ArrayList;
import java.util.List;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.query.Query;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;

/**
 * GroupByContinueReceiver
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-09
 */
public class GroupByContinueReceiver extends BaseContinueReceiver {
    @Override
    protected String getAtomCubeMetric(Query query) {
        return getAtomCubeMetric(((GroupByQuery) query).getAggregatorSpecs());
    }

    @Override
    protected List<AtomCubeRow> receive(String metric, Query query, Object input) {
        List<AtomCubeRow> list = new ArrayList<>();
        if (input instanceof ResultRow && query instanceof GroupByQuery) {
            ResultRow resultRow = (ResultRow) input;
            MapBasedRow basedRow = resultRow.toMapBasedRow((GroupByQuery) query);

            AtomCubeRow row = new AtomCubeRow(basedRow.getTimestamp().toString(), basedRow.getEvent());
            row.setValue(toMutableRoaringBitmap(row.get(metric)));
            row.remove(metric);
            list.add(row);
        }
        return list;
    }
}
