package com.kxw.zookeeper.single;
/**
 * AbstractZooKeeper.java
 * 版权所有(C) 2013
 * 创建:cuiran 2013-01-16 14:59:44
 */

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;


/**
 * {<a href='http://blog.csdn.net/cuiran/article/details/8509642'>@link</a>}
 * @author cuiran
 */
public class AbstractZooKeeper implements Watcher {
    private static Log log = LogFactory.getLog(AbstractZooKeeper.class.getName());

    //缓存时间
    private static final int SESSION_TIME = 2000;
    protected ZooKeeper zooKeeper;
    protected CountDownLatch countDownLatch = new CountDownLatch(1);

    public void connect(String hosts) throws IOException, InterruptedException {
        System.out.println("connect and await ...");
        zooKeeper = new ZooKeeper(hosts, SESSION_TIME, this);
        countDownLatch.await();
    }

    /* (non-Javadoc)
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    public void process(WatchedEvent event) {
        System.out.println("process ...");
        /**
         * Disconnected : The client is in the disconnected state - it is not connected to any server in the ensemble.
         * Expired : The serving cluster has expired this session.
         * NoSyncConnected : Deprecated.
         * SyncConnected : The client is in the connected state - it is connected to a server in the ensemble (one of the servers specified in the host connection parameter during ZooKeeper client creation).
         * Unknown : Deprecated.
         */
        if (event.getState() == KeeperState.SyncConnected) {
            countDownLatch.countDown();
            System.out.println("connect countdown ...");
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
