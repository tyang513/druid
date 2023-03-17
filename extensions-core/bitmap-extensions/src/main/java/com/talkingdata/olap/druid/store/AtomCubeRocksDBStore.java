package com.talkingdata.olap.druid.store;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.talkingdata.bitmapservice.BitmapOperation;
import com.talkingdata.bitmapservice.client.BitmapServiceClient;
import com.talkingdata.olap.druid.BitmapUtil;
import com.talkingdata.olap.druid.DebugLogger;
import com.talkingdata.olap.druid.DebugLoggerFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.collections.StupidResourceHolder;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.thrift.TException;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * AtomCubeRocksDBStore
 *
 * @author minfengxu
 * @version v1
 * @since 2017/4/20
 */
public class AtomCubeRocksDBStore implements AtomCubeStore {
    private static final DebugLogger LOGGER = DebugLoggerFactory.getLogger(AtomCubeRocksDBStore.class);
    private final Supplier<ResourceHolder<BitmapServiceClient>> client;

    public AtomCubeRocksDBStore(Supplier<ResourceHolder<BitmapServiceClient>> clientSupplier,
                                AtomCubeRocksDBStoreConfig config) {
        client = clientSupplier;
    }

    public static AtomCubeRocksDBStore create(final AtomCubeRocksDBStoreConfig config) {
        LOGGER.debug("Creating rocksDB Client", config.getHosts(), config.getNumConnections());

        try {
            String[] array = config.getHosts().split(":");
            Supplier<ResourceHolder<BitmapServiceClient>> clientSupplier = Suppliers.ofInstance(
                    StupidResourceHolder.create(BitmapServiceClient.of(array[0], Integer.parseInt(array[1]))));
            return new AtomCubeRocksDBStore(clientSupplier, config);
        } catch (Exception e) {
            throw new IAE(e, "Created rocksDB client error");
        }
    }

    @Override
    public MutableRoaringBitmap get(List<String> keys, DataSetOperation dataSetOperation) {
        if (keys == null || keys.isEmpty()) {
            return null;
        }

        ByteBuffer byteBuffer = null;
        try {
            if (dataSetOperation == null) {
                byteBuffer = client
                        .get()
                        .get()
                        .get(ByteBuffer.wrap(keys.get(0).getBytes()));
            } else {
                List<ByteBuffer> bytes = keys.stream()
                        .map(r -> ByteBuffer.wrap(r.getBytes()))
                        .collect(Collectors.toList());

                byteBuffer = client
                        .get()
                        .get()
                        .operation(bytes, BitmapOperation.valueOf(dataSetOperation.name()));
            }
        } catch (TException e) {
        }

        if (byteBuffer != null) {
            return new ImmutableRoaringBitmap(byteBuffer).toMutableRoaringBitmap();
        }
        return new MutableRoaringBitmap();
    }

    @Override
    public void put(String key, RoaringBitmap bitmap) {
        try {
            ByteBuffer keyBytes = ByteBuffer.wrap(key.getBytes());
            ByteBuffer valueBytes = ByteBuffer.wrap(BitmapUtil.toBytes(bitmap));
            client.get().get().put(keyBytes, valueBytes);
        } catch (TException e) {
            throw new ISE(e.getMessage());
        }
    }
}
