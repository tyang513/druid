package com.talkingdata.olap.druid.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;

/**
 * AtomCubeRocksDBStoreConfig
 *
 * @author minfengxu
 * @version v1
 * @since 2017/4/20
 */
public class AtomCubeRocksDBStoreConfig {
    @JsonProperty
    @NotNull
    private String hosts;

    // size of rocksdb connection pool
    @JsonProperty
    private int numConnections = 1;

    public String getHosts() {
        return hosts;
    }

    public int getNumConnections() {
        return numConnections;
    }
}
