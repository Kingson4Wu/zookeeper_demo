package com.kxw.observer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Observable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import static com.kxw.observer.ZkConstants.ZK_ROOT_NODE;

@Component
public class ZookeeperService implements CuratorWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperService.class);

    private CuratorFramework zkClient;

    private final Map<String, ZooKeeperObservable> observableMap =
        new HashMap<>();

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private CommonExecutorService commonExecutorService;

    public void init() {

        commonExecutorService.submit(() -> {
            try {
                Thread.sleep(1000L * 3);

                registerZkObserver();

                String zkServersString = "127.0.0.1:3333,127.0.0.1:3333,127.0.0.1:3333";
                //String zkServersString = zkConfig.getZkServersAddress();
                connectZooKeeper(zkServersString, 15 * 1000);

            } catch (Exception e) {
                LOGGER.error("ZookeeperService.init ", e);
            }
        });
    }

    public void destroy() {
        if (Objects.nonNull(zkClient)) {
            zkClient.close();
        }
    }

    private void connectZooKeeper(String connectString, int connectionTimeoutMs) {

        zkClient = CuratorFrameworkFactory.builder()
            .connectString(connectString)
            .connectionTimeoutMs(connectionTimeoutMs)
            .retryPolicy(new ExponentialBackoffRetry(4000, 29))
            .build();
        zkClient.getConnectionStateListenable().addListener((client, newState) -> {
            if (newState == ConnectionState.CONNECTED) {
                createPreZooKeeperNode(ZK_ROOT_NODE);
                /**createPreZooKeeperNode(getRealWatchedNode(ZK_DATA_NODE));
                 registerWatchedNode(getRealWatchedNode(ZK_DATA_NODE));*/

                //if (MapUtils.isNotEmpty(observableMap)) {
                if (observableMap != null) {
                    for (Map.Entry<String, ZooKeeperObservable> observableEntry : observableMap.entrySet()) {

                        String watchNode = observableEntry.getKey();
                        String realWatchNode = getRealWatchedNode(watchNode);
                        createPreZooKeeperNode(realWatchNode);
                        registerWatchedNode(realWatchNode);
                    }
                }
            }
        });
        zkClient.start();
    }

    private synchronized void registerZkObserver() {
        Map<String, ZooKeeperObserver> zooKeeperObserverMap = applicationContext.getBeansOfType(
            ZooKeeperObserver.class);
        if (zooKeeperObserverMap != null) {
            //if (MapUtils.isNotEmpty(zooKeeperObserverMap)) {
            for (Map.Entry<String, ZooKeeperObserver> zooKeeperObserverEntry : zooKeeperObserverMap.entrySet()) {
                ZooKeeperObserver observer = zooKeeperObserverEntry.getValue();
                String watchNode = observer.getWatchedNode();
                ZooKeeperObservable observable = observableMap.computeIfAbsent(watchNode,
                    k -> new ZooKeeperObservable());
                observable.addObserver(observer);
            }

        }
    }

    //------

    private void createPreZooKeeperNode(String zkNode) {
        try {
            if (zkClient.checkExists().forPath(zkNode) == null) {
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(zkNode, new byte[0]);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void registerWatchedNode(String watchedNode) {
        try {
            if (zkClient.checkExists().forPath(watchedNode) == null) {
                zkClient.create().forPath(watchedNode);
            }
        } catch (Exception e) {
            LOGGER.error("ZookeeperService.registerWatchedNode ", e);
        }
        try {
            zkClient.getData().usingWatcher(this).forPath(watchedNode);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("ZookeeperService.registerWatchedNode add watched node: {}", watchedNode);
            }
        } catch (Exception e) {
            LOGGER.error("ZookeeperService.registerWatchedNode ", e);
        }
    }

    public void setData(String zkNode, byte[] data) {

        if (zkClient == null) {
            LOGGER.error("zookeeper client is null.");
            return;
        }
        String realZkNode = getRealWatchedNode(zkNode);
        try {
            zkClient.setData().forPath(realZkNode, data);
        } catch (Exception e) {
            LOGGER.error("ZookeeperService.setData", e);
        }
    }

    protected String getRealWatchedNode(String watchedNode) {
        return ZK_ROOT_NODE + watchedNode;
    }

    private String getObserverWatchedNode(String realWatchedNode) {
        int startIndex = realWatchedNode.indexOf(ZK_ROOT_NODE);
        if (startIndex >= 0) {
            return realWatchedNode.substring(startIndex + ZK_ROOT_NODE.length());
        }
        return realWatchedNode;
    }

    //----

    @Override
    public void process(WatchedEvent event) throws Exception {
        try {
            if (event == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("ZookeeperService.process event is null.");
                }
                return;
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("ZookeeperService.process receive {} event.", event.getType());
            }
            switch (event.getType()) {
                case None:
                case NodeChildrenChanged:
                case NodeCreated:
                    break;
                case NodeDataChanged:

                    String observerWatchedNode = getObserverWatchedNode(event.getPath());
                    ZooKeeperObservable observable = observableMap.get(observerWatchedNode);
                    if (observable == null) {
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn("ZookeeperService observable[path={}] not found.", event.getPath());
                        }
                        return;
                    }
                    byte[] data = zkClient.getData().usingWatcher(this).forPath(event.getPath());
                    observable.update(data);
                    break;
                default:

            }
        } catch (Exception e) {
            LOGGER.error("ZookeeperService ", e);
        }
    }

    private class ZooKeeperObservable extends Observable {

        public void update(byte[] data) {
            setChanged();
            notifyObservers(data);
        }
    }
}
