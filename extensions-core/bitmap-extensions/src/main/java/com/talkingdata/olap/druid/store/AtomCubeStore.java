package com.talkingdata.olap.druid.store;

import java.util.List;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * AtomCubeStore
 *
 * @author minfengxu
 * @version v1
 * @since 2017/4/20
 */
public interface AtomCubeStore {

    MutableRoaringBitmap get(List<String> keys, DataSetOperation dataSetOperation);

    void put(String key, RoaringBitmap bitmap);
}
