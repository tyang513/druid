package com.talkingdata.olap.druid.serde;

import com.talkingdata.olap.druid.BitmapUtil;
import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import javax.annotation.Nullable;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;
import org.roaringbitmap.RoaringBitmap;

/**
 * AtomCubeAggregateCombiner
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-01
 */
public final class AtomCubeAggregateCombiner extends ObjectAggregateCombiner<RoaringBitmap> {
    private static final DebugLogger TRACER = DebugLoggerFactory.getLogger(AtomCubeAggregateCombiner.class);

    private RoaringBitmap combined;

    @Override
    public void reset(ColumnValueSelector selector) {
        combined = null;
        fold(selector);
    }

    @Override
    public void fold(ColumnValueSelector selector) {
        TRACER.debug("Folding...");
        Object value = selector.getObject();
        if (value == null) {
            return;
        }

        if (combined == null) {
            combined = new RoaringBitmap();
        }

        combined = BitmapUtil.union(combined, (RoaringBitmap) value);
    }

    @Override
    public Class<RoaringBitmap> classOfObject() {
        return RoaringBitmap.class;
    }

    @Nullable
    @Override
    public RoaringBitmap getObject() {
        return combined;
    }
}
