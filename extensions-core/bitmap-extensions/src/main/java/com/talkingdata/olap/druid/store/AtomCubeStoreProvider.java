package com.talkingdata.olap.druid.store;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Provider;

/**
 * AtomCubeStoreProvider
 *
 * @author minfengxu
 * @version v1
 * @since 2017/4/20
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = AtomCubeRocksDBStoreProvider.class)
@JsonSubTypes(value = {@JsonSubTypes.Type(name = "rocksdb", value = AtomCubeRocksDBStoreProvider.class)})
public interface AtomCubeStoreProvider extends Provider<AtomCubeStore> {
}
