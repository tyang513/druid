package com.talkingdata.olap.druid.store;

/**
 * AtomCubeRocksDBStoreProvider
 *
 * @author minfengxu
 * @version v1
 * @since 2017/4/20
 */
public class AtomCubeRocksDBStoreProvider extends AtomCubeRocksDBStoreConfig implements AtomCubeStoreProvider {
    @Override
    public AtomCubeStore get() {
        return AtomCubeRocksDBStore.create(this);
    }
}
