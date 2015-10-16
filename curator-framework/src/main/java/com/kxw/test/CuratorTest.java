package com.kxw.test;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

/**
 * 使用curator监听zookeeper节点
 *
 * @author qindongliang
 **/
public class CuratorTest {

    static CuratorFramework zkclient = null;
    static String nameSpace = "php";

    static {

        String zkhost = "127.0.0.1:2181";//zk的host
        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);//重试机制
        Builder builder = CuratorFrameworkFactory.builder().connectString(zkhost)
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .retryPolicy(rp);
        builder.namespace(nameSpace);
        CuratorFramework zclient = builder.build();
        zkclient = zclient;
        zkclient.start();// 放在这前面执行
        zkclient.newNamespaceAwareEnsurePath(nameSpace);

    }

    public static void main(String[] args) throws Exception {

        watch();
        Thread.sleep(Long.MAX_VALUE);

    }

    /**
     * 监听节点变化
     */
    public static void watch() throws Exception {
        PathChildrenCache cache = new PathChildrenCache(zkclient, "/zk", false);
        cache.start();

        System.out.println("监听开始/zk........");
        PathChildrenCacheListener plis = new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                    throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }

                    case CHILD_UPDATED: {
                        System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }

                    case CHILD_REMOVED: {
                        System.out.println("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                }

            }
        };
//注册监听
        cache.getListenable().addListener(plis);

    }

}
