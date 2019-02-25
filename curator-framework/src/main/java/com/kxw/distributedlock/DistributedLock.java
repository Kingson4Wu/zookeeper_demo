package com.kxw.distributedlock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by kingsonwu on 18/3/7.
 * <a href='http://developer.51cto.com/art/201710/555187.htm'>@link</a>
 */
public class DistributedLock {

    public static void main(String[] args) throws Exception {
        //创建zookeeper的客户端
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
            "10.21.41.181:2181,10.21.42.47:2181,10.21.49.252:2181", retryPolicy);
        client.start();

        //创建分布式锁, 锁空间的根节点路径为/curator/lock
        InterProcessMutex mutex = new InterProcessMutex(client, "/curator/lock");
        mutex.acquire();
        //获得了锁, 进行业务流程
        System.out.println("Enter mutex");
        //完成业务流程, 释放锁
        mutex.release();

        //关闭客户端
        client.close();
    }

    //internalLockLoop：阻塞等待直到获得锁。
}
