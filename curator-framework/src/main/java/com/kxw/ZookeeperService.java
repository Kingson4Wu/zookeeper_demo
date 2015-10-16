package com.kxw;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Kingson.wu on 2015/10/15.
 */
public class ZookeeperService implements CuratorWatcher {

    private CuratorFramework zkClient;

    private final Logger logger = LoggerFactory.getLogger(getClass());


    public void init(){

        zkClient = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(4000, 29))
                .build();
        zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState == ConnectionState.CONNECTED) {
                    createPreZooKeeperNode("/config");
                    createPreZooKeeperNode("/config/kxw");
                    registerWatchedNode("/config");
                    registerWatchedNode("/config/kxw");
                }
            }
        });
        zkClient.start();

    }

    @Override
    public void process(WatchedEvent event) throws Exception {
        byte[] data;
        String path;

        try {
            if (event == null) {
                if (logger.isWarnEnabled()) {
                    logger.warn("process event is null.");
                }
                return;
            }
            if (logger.isInfoEnabled()) {
                logger.info("process receive {} event.", event.getType());
            }
            switch (event.getType()) {

                case None:
                case NodeChildrenChanged:
                case NodeCreated:
                    path = event.getPath();
                    System.out.println("Node" + path + "create ...");

                case NodeDataChanged:
                    /** some operation**/
                    path = event.getPath();
                    data = zkClient.getData().usingWatcher(this).forPath(event.getPath());
                    System.out.println("Node" + path + "change ..." + data);
                    //observable.update(data);
                    break;

            }
        } catch (Throwable e) {
            logger.error("Error happen :", e);
        }
    }

    private void createPreZooKeeperNode(String zkNode){
        try {
            if (zkClient.checkExists().forPath(zkNode) == null) {
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(zkNode, new byte[0]);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private void registerWatchedNode(String watchedNode) {
        try {
            if (zkClient.checkExists().forPath(watchedNode) == null) {
                zkClient.create().forPath(watchedNode);
            }
        } catch (Throwable e) {
            logger.error("Error happen ： ", e);
        }
        try {
            zkClient.getData().usingWatcher(this).forPath(watchedNode);
            if (logger.isInfoEnabled()) {
                logger.info("add watched node: {}", watchedNode);
            }
        } catch (Throwable e) {
            logger.error("Error happen ： ", e);
        }
    }


    public static void main(String[] args) {

        ZookeeperService zkService = new ZookeeperService();
        zkService.init();
        zkService.createPreZooKeeperNode("/kxw");


    }
}
