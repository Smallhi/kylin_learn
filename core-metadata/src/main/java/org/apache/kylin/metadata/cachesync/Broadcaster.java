/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.metadata.cachesync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

// 如何在分布式系统中缓存同步，怎么创建，怎么更新，怎么删除，怎么同步，怎么通告，怎么响应？
// 以下是注解翻译
// 在Kylin的服务集群上广播元数据的变化
//  原始服务通过Rest API通报Kylin服务器（包括自己）（变化）事件。
//  对于目标服务来说，监听器被注册用来处理接收的事件。作为处理的一部分，监听器能重新通知其他本地的监听器一个新的事件
// 这里有几个概念 Origin Server / Target Server / announce/ Listeners/ listeners register

// 一个代男性的项目空间变化事件有：
// - 模型在原始服务器上更新，那么，一个model更新事件被通报
// - 在所有的服务上，model 监听器被调用，重载模型，并且告知一个项目空间更新事件（接受通告，处理，并告知）
// - 所有的监听器相应项目空间更新，--重载cube描述，清理项目L2级别缓存，清理Calcite数据源等等。



/**
 * Broadcast metadata changes across all Kylin servers.
 *
 * The origin server announce the event via Rest API to all Kylin servers including itself.
 * On target server, listeners are registered to process events. As part of processing, a
 * listener can re-notify a new event to other local listeners.
 *
 * A typical project schema change event:
 * - model is update on origin server, a "model" update event is announced
 * - on all servers, model listener is invoked, reload the model, and notify a "project_schema" update event
 * - all listeners respond to the "project_schema" update -- reload cube desc, clear project L2 cache, clear calcite data source etc
 */
public class Broadcaster {

    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    public static final String SYNC_ALL = "all"; // the special entity to indicate clear all
    public static final String SYNC_PRJ_SCHEMA = "project_schema"; // the special entity to indicate project schema has change, e.g. table/model/cube_desc update
    public static final String SYNC_PRJ_DATA = "project_data"; // the special entity to indicate project data has change, e.g. cube/raw_table update
    public static final String SYNC_PRJ_ACL = "project_acl"; // the special entity to indicate query ACL has change, e.g. table_acl/learn_kylin update

    public static Broadcaster getInstance(KylinConfig config) {
        return config.getManager(Broadcaster.class);
    }

    // called by reflection
    static Broadcaster newInstance(KylinConfig config) {
        return new Broadcaster(config);
    }

    // ============================================================================

    static final Map<String, List<Listener>> staticListenerMap = Maps.newConcurrentMap();

    private KylinConfig config;
    private ExecutorService announceMainLoop;
    private ExecutorService announceThreadPool;
    private SyncErrorHandler syncErrorHandler;

    // 广播的事件一个链表的阻塞队列
    // TODO LinkedBlockingDeque特性
    private BlockingDeque<BroadcastEvent> broadcastEvents = new LinkedBlockingDeque<>();
    private Map<String, List<Listener>> listenerMap = Maps.newConcurrentMap();
    private AtomicLong counter = new AtomicLong(); // a counter for testing purpose

    private Broadcaster(final KylinConfig config) {
        this.config = config;
        this.syncErrorHandler = getSyncErrorHandler(config);
        // TODO newSingleThreadExecutor特性

        //通告服务线程
        // 通告主循环，使用Java多线程建立一个单线程的线程池，将多个任务交个这个线程池，会一个接一个的处理，如果出现异常，会有一个新的线程替代
        this.announceMainLoop = Executors.newSingleThreadExecutor(new DaemonThreadFactory());

        // 通告服务处理线程池
        // 这个线程池是要通过多线程的方式通告多个服务
        this.announceThreadPool = new ThreadPoolExecutor(1, 10, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory());
        // 读取到Kylin.properties 中所有的Rest服务器 实际就是参数kylin.server.cluster-server
        final String[] nodes = config.getRestServers();
        if (nodes == null || nodes.length < 1) {
            logger.warn("There is no available rest server; check the 'kylin.server.cluster-servers' config");
        }
        logger.debug(nodes.length + " nodes in the cluster: " + Arrays.toString(nodes));

        // Executors execut是父类的抽象方法，需实现
        announceMainLoop.execute(new Runnable() {
            @Override
            // run 是实际运行的命令
            public void run() {
                // 使用一个Map存储所有的服务的restClient.
                final Map<String, RestClient> restClientMap = Maps.newHashMap();

                //死循环，如果通告服务线程池没有不shutdown，就一直处理
                while (!announceThreadPool.isShutdown()) {
                    try {
                        // 处理逻辑是：
                        // 1. 从阻塞队列事件中拿到队首的事件
                        // 2. 从配置文件中拿到所有的服务url
                        // 3. 如果restClient Map为空，就把这些URL加到Map中，
                        // 4. 判断广播事件的目的节点，如果为空，就默认通知所有节点，如果不是通知所有或者是单节点
                        // ，就跳出循环，不执行通告（这个地方是单机版和集群版的区别）
                        // 5. 通告服务线程池启动，通过rest客户封装的方法wipeCache将时间按方出去。并进行容错处理
                        // 说明通告事件的参数有：实体，事件和缓存key值。

                        // 事件怎么写入的，通告在创建实例的同时，需要注册监听器，监听器的作用就是监听事件变化，一旦有变化就把事件加到队列里面，由通报器发出去。
                        // 监听器是个抽象类，需要在具体类中实现，
                        // 实际上，缓存同步的方法是，先由缓存调put方法通告所有的服务。服务上的再调用监听
                        // 最后对外暴露的服务提供通知监听器的方法，也就是在restAPI接收到请求之后告知监听器跑对应逻辑。

                        final BroadcastEvent broadcastEvent = broadcastEvents.takeFirst();

                        String[] restServers = config.getRestServers();
                        logger.debug("Servers in the cluster: " + Arrays.toString(restServers));
                        for (final String node : restServers) {
                            if (restClientMap.containsKey(node) == false) {
                                restClientMap.put(node, new RestClient(node));
                            }
                        }

                        String toWhere = broadcastEvent.getTargetNode();
                        if (toWhere == null)
                            toWhere = "all";
                        logger.debug("Announcing new broadcast to " + toWhere + ": " + broadcastEvent);

                        for (final String node : restServers) {
                            if (!(toWhere.equals("all") || toWhere.equals(node)))
                                continue;

                            announceThreadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    RestClient restClient = restClientMap.get(node);
                                    try {
                                        restClient.wipeCache(broadcastEvent.getEntity(), broadcastEvent.getEvent(),
                                                broadcastEvent.getCacheKey());
                                    } catch (IOException e) {
                                        logger.error(
                                                "Announce broadcast event failed, targetNode {} broadcastEvent {}, error msg: {}",
                                                node, broadcastEvent, e);
                                        syncErrorHandler.handleAnnounceError(node, restClient, broadcastEvent);
                                    }
                                }
                            });
                        }
                    } catch (Exception e) {
                        logger.error("error running wiping", e);
                    }
                }
            }
        });
    }

    private SyncErrorHandler getSyncErrorHandler(KylinConfig config) {
        String clzName = config.getCacheSyncErrorHandler();
        if (StringUtils.isEmpty(clzName)) {
            clzName = DefaultSyncErrorHandler.class.getName();
        }
        return (SyncErrorHandler) ClassUtil.newInstance(clzName);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public void stopAnnounce() {
        announceThreadPool.shutdown();
        announceMainLoop.shutdown();
    }

    // static listener survives cache wipe and goes after normal listeners
    public void registerStaticListener(Listener listener, String... entities) {
        doRegisterListener(staticListenerMap, listener, entities);
    }

    public void registerListener(Listener listener, String... entities) {
        doRegisterListener(listenerMap, listener, entities);
    }

    private static void doRegisterListener(Map<String, List<Listener>> lmap, Listener listener, String... entities) {
        synchronized (lmap) {
            // ignore re-registration
            List<Listener> all = lmap.get(SYNC_ALL);
            if (all != null && all.contains(listener)) {
                return;
            }

            for (String entity : entities) {
                if (!StringUtils.isBlank(entity))
                    addListener(lmap, entity, listener);
            }
            addListener(lmap, SYNC_ALL, listener);
            addListener(lmap, SYNC_PRJ_SCHEMA, listener);
            addListener(lmap, SYNC_PRJ_DATA, listener);
            addListener(lmap, SYNC_PRJ_ACL, listener);
        }
    }

    private static void addListener(Map<String, List<Listener>> lmap, String entity, Listener listener) {
        List<Listener> list = lmap.get(entity);
        if (list == null) {
            list = new ArrayList<>();
            lmap.put(entity, list);
        }
        list.add(listener);
    }

    public void notifyClearAll() throws IOException {
        notifyListener(SYNC_ALL, Event.UPDATE, SYNC_ALL);
    }

    public void notifyProjectSchemaUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_SCHEMA, Event.UPDATE, project);
    }

    public void notifyProjectDataUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_DATA, Event.UPDATE, project);
    }

    public void notifyProjectACLUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_ACL, Event.UPDATE, project);
    }

    public void notifyListener(String entity, Event event, String cacheKey) throws IOException {
        notifyListener(entity, event, cacheKey, true);
    }

    public void notifyNonStaticListener(String entity, Event event, String cacheKey) throws IOException {
        notifyListener(entity, event, cacheKey, false);
    }

    private void notifyListener(String entity, Event event, String cacheKey, boolean includeStatic) throws IOException {
        // prevents concurrent modification exception
        List<Listener> list = Lists.newArrayList();
        List<Listener> l1 = listenerMap.get(entity); // normal listeners first
        if (l1 != null)
            list.addAll(l1);

        if (includeStatic) {
            List<Listener> l2 = staticListenerMap.get(entity); // static listeners second
            if (l2 != null)
                list.addAll(l2);
        }

        if (list.isEmpty())
            return;

        logger.debug("Broadcasting " + event + ", " + entity + ", " + cacheKey);

        switch (entity) {
        case SYNC_ALL:
            for (Listener l : list) {
                l.onClearAll(this);
            }
            config.clearManagers(); // clear all registered managers in config
            break;
        case SYNC_PRJ_SCHEMA:
            ProjectManager.getInstance(config).clearL2Cache(cacheKey);
            for (Listener l : list) {
                l.onProjectSchemaChange(this, cacheKey);
            }
            break;
        case SYNC_PRJ_DATA:
            ProjectManager.getInstance(config).clearL2Cache(cacheKey); // cube's first becoming ready leads to schema change too
            for (Listener l : list) {
                l.onProjectDataChange(this, cacheKey);
            }
            break;
        case SYNC_PRJ_ACL:
            ProjectManager.getInstance(config).clearL2Cache(cacheKey);
            for (Listener l : list) {
                l.onProjectQueryACLChange(this, cacheKey);
            }
            break;
        default:
            for (Listener l : list) {
                l.onEntityChange(this, entity, event, cacheKey);
            }
            break;
        }

        logger.debug("Done broadcasting " + event + ", " + entity + ", " + cacheKey);
    }

    /**
     * Announce an event out to peer kylin servers
     */
    public void announce(String entity, String event, String key) {
        announce(new BroadcastEvent(entity, event, key));
    }

    public void announce(BroadcastEvent event) {
        if (broadcastEvents == null)
            return;

        try {
            counter.incrementAndGet();
            broadcastEvents.putLast(event);
        } catch (Exception e) {
            counter.decrementAndGet();
            logger.error("error putting BroadcastEvent", e);
        }
    }

    public long getCounterAndClear() {
        return counter.getAndSet(0);
    }

    // ============================================================================

    public static class DefaultSyncErrorHandler implements SyncErrorHandler {
        Broadcaster broadcaster;
        int maxRetryTimes;

        @Override
        public void init(Broadcaster broadcaster) {
            this.maxRetryTimes = broadcaster.getConfig().getCacheSyncRetrys();
            this.broadcaster = broadcaster;
        }

        @Override
        public void handleAnnounceError(String targetNode, RestClient restClient, BroadcastEvent event) {
            int retry = event.getRetryTime() + 1;

            // when sync failed, put back to queue to retry
            if (retry < maxRetryTimes) {
                event.setRetryTime(retry);
                event.setTargetNode(targetNode);
                broadcaster.announce(event);
            } else {
                logger.error("Announce broadcast event exceeds retry limit, abandon targetNode {} broadcastEvent {}",
                        targetNode, event);
            }
        }
    }

    public interface SyncErrorHandler {
        void init(Broadcaster broadcaster);

        void handleAnnounceError(String targetNode, RestClient restClient, BroadcastEvent event);
    }

    public enum Event {

        CREATE("create"), UPDATE("update"), DROP("drop");
        private String text;

        Event(String text) {
            this.text = text;
        }

        public String getType() {
            return text;
        }

        public static Event getEvent(String event) {
            for (Event one : values()) {
                if (one.getType().equalsIgnoreCase(event)) {
                    return one;
                }
            }

            return null;
        }
    }

    abstract public static class Listener {
        public void onClearAll(Broadcaster broadcaster) throws IOException {
        }

        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onProjectDataChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onProjectQueryACLChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
        }
    }

    public static class BroadcastEvent {
        private int retryTime;
        private String targetNode; // NULL means to all

        private String entity;
        private String event;
        private String cacheKey;

        public BroadcastEvent(String entity, String event, String cacheKey) {
            super();
            this.entity = entity;
            this.event = event;
            this.cacheKey = cacheKey;
        }

        public int getRetryTime() {
            return retryTime;
        }

        public void setRetryTime(int retryTime) {
            this.retryTime = retryTime;
        }

        public String getTargetNode() {
            return targetNode;
        }

        public void setTargetNode(String targetNode) {
            this.targetNode = targetNode;
        }

        public String getEntity() {
            return entity;
        }

        public String getEvent() {
            return event;
        }

        public String getCacheKey() {
            return cacheKey;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((event == null) ? 0 : event.hashCode());
            result = prime * result + ((cacheKey == null) ? 0 : cacheKey.hashCode());
            result = prime * result + ((entity == null) ? 0 : entity.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (this == obj) {
                return true;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BroadcastEvent other = (BroadcastEvent) obj;
            if (!StringUtils.equals(event, other.event)) {
                return false;
            }
            if (!StringUtils.equals(cacheKey, other.cacheKey)) {
                return false;
            }
            if (!StringUtils.equals(entity, other.entity)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("entity", entity).add("event", event).add("cacheKey", cacheKey)
                    .toString();
        }

    }
}
