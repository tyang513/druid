package com.talkingdata.olap.druid;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.talkingdata.olap.druid.post.AtomCubeRawPostAggregator;
import com.talkingdata.olap.druid.post.AtomCubeSetPostAggregator;
import com.talkingdata.olap.druid.post.AtomCubeSizePostAggregator;
import com.talkingdata.olap.druid.query.AtomCubeDataSetQuery;
import com.talkingdata.olap.druid.query.AtomCubeDataSetQueryToolChest;
import com.talkingdata.olap.druid.query.AtomCubeQuery;
import com.talkingdata.olap.druid.query.AtomCubeQueryResource;
import com.talkingdata.olap.druid.query.AtomCubeQueryToolChest;
import com.talkingdata.olap.druid.serde.AtomCube;
import com.talkingdata.olap.druid.serde.AtomCubeAggregatorFactory;
import com.talkingdata.olap.druid.serde.AtomCubeSerializer;
import com.talkingdata.olap.druid.store.AtomCubeStoreModule;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.Query;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.server.NoopQuerySegmentWalker;
import org.roaringbitmap.RoaringBitmap;

/**
 * AtomCubeDruidModule
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class AtomCubeDruidModule implements DruidModule {
    private static final DebugLogger TRACER = DebugLoggerFactory.getLogger(AtomCubeDruidModule.class);
    private static final Map<String, NodeRole> COMMAND_TYPE;

    static {
        COMMAND_TYPE = new HashMap<>();

        COMMAND_TYPE.put("org.apache.druid.cli.Main server coordinator", NodeRole.COORDINATOR);
        COMMAND_TYPE.put("org.apache.druid.cli.Main server historical", NodeRole.HISTORICAL);
        COMMAND_TYPE.put("org.apache.druid.cli.Main server middleManager", NodeRole.MIDDLE_MANAGER);
        COMMAND_TYPE.put("org.apache.druid.cli.Main internal peon", NodeRole.PEON);
        COMMAND_TYPE.put("org.apache.druid.cli.Main server broker", NodeRole.BROKER);
        COMMAND_TYPE.put("org.apache.druid.cli.Main server router", NodeRole.ROUTER);
    }

    private static NodeRole NODE_ROLE;

    @Override
    public void configure(Binder binder) {
        TRACER.debug("===================== Configuring ====================", this, binder);

        ComplexMetrics.registerSerde(AtomCube.TYPE_NAME, new AtomCube());

        MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);
        toolChests.addBinding(AtomCubeQuery.class).to(AtomCubeQueryToolChest.class);
        toolChests.addBinding(AtomCubeDataSetQuery.class).to(AtomCubeDataSetQueryToolChest.class);
        binder.bind(AtomCubeQueryToolChest.class).in(LazySingleton.class);
        binder.bind(AtomCubeDataSetQueryToolChest.class).in(LazySingleton.class);

        JsonConfigProvider.bind(binder, "druid.broker.cache", CacheConfig.class);
        binder.install(new CacheModule());
        binder.install(new AtomCubeStoreModule());

        String command = System.getProperty("sun.java.command");
        NODE_ROLE = parseNodeRole(command);
        TRACER.debug("Configuring role: " + NODE_ROLE, this);
        if (NODE_ROLE != null && NODE_ROLE == NodeRole.BROKER) {
            Jerseys.addResource(binder, AtomCubeQueryResource.class);
            LifecycleModule.register(binder, AtomCubeQueryResource.class);
        }
    }

    private NodeRole parseNodeRole(String command) {
        for (Map.Entry<String, NodeRole> entry : COMMAND_TYPE.entrySet()) {
            if (command.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    @Override
    public List<? extends Module> getJacksonModules() {
        TRACER.debug("Creating module", this);

        return Collections.<Module>singletonList(new SimpleModule("AtomCubeDruidModule")
                .registerSubtypes(
                        new NamedType(AtomCubeAggregatorFactory.class, AtomCube.TYPE_NAME),
                        new NamedType(AtomCubeQuery.class, AtomCube.TYPE_NAME),
                        new NamedType(AtomCubeRawPostAggregator.class, AtomCubeRawPostAggregator.TYPE_NAME),
                        new NamedType(AtomCubeSizePostAggregator.class, AtomCubeSizePostAggregator.TYPE_NAME),
                        new NamedType(AtomCubeSetPostAggregator.class, AtomCubeSetPostAggregator.TYPE_NAME),
                        new NamedType(AtomCubeDataSetQuery.class, AtomCubeDataSetQuery.TYPE_NAME))
                .addSerializer(RoaringBitmap.class, new AtomCubeSerializer<>()));
    }

    @Provides
    @LazySingleton
    @Named(AtomCube.TYPE_NAME)
    public QuerySegmentWalker createQueryWalker(Injector injector) {
        TRACER.info("Current node role: " + NODE_ROLE);
        if (NODE_ROLE != null) {
            if (NODE_ROLE == NodeRole.BROKER
                    || NODE_ROLE == NodeRole.HISTORICAL
                    || NODE_ROLE == NodeRole.PEON) {
                return injector.getInstance(QuerySegmentWalker.class);
            }
        }
        return new NoopQuerySegmentWalker();
    }

    @Provides
    @LazySingleton
    @Named(AtomCube.TYPE_NAME)
    public QueryWatcher createQueryWatcher(Injector injector) {
        TRACER.info("Current node role: " + NODE_ROLE);
        if (NODE_ROLE != null) {
            if (NODE_ROLE == NodeRole.BROKER
                    || NODE_ROLE == NodeRole.HISTORICAL
                    || NODE_ROLE == NodeRole.PEON) {
                return injector.getInstance(QueryWatcher.class);
            }
        }
        return (Query<?> query, ListenableFuture<?> future) -> {
        };
    }
}
