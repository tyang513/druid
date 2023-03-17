package com.talkingdata.olap.druid.query;

import java.util.List;
import org.apache.druid.java.util.common.guava.Yielder;

/**
 * BitmapConditionParser
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-09
 */
public interface ContinueReceiver<Query> {
    List<AtomCubeRow> receive(Query query, Yielder yielder);
}
