package com.talkingdata.olap.druid.store;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Global;

/**
 * AtomCubeStoreModule
 *
 * @author minfengxu
 * @version v1
 * @since 2017/4/20
 */
public class AtomCubeStoreModule implements Module {
    private static final String PREFIX = "druid.atomcube.store";

    @Override
    public void configure(Binder binder) {
        binder.bind(AtomCubeStore.class)
                .toProvider(Key.get(AtomCubeStoreProvider.class, Global.class))
                .in(ManageLifecycle.class);
        JsonConfigProvider.bind(binder, PREFIX, AtomCubeStoreProvider.class, Global.class);
    }
}
