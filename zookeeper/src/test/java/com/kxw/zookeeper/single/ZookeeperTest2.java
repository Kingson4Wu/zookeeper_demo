package com.kxw.zookeeper.single;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;

/**
 * Created by Kingson.wu on 2015/10/8.
 */
public class ZookeeperTest2 {

     public static void main( String[] args ) throws IOException, KeeperException, InterruptedException {

         //2.
         // 创建一个与服务器的连接
              ZooKeeper zk = new ZooKeeper("localhost:" + 2181,
                       1000, new Watcher() {
	                       // 监控所有被触发的事件
	                       public void process(WatchedEvent event) {
	                           System.out.println("已经触发了" + event.getType() + "事件！");
	                       }
	                   });
	            // 创建一个目录节点
	            zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE,
	              CreateMode.PERSISTENT);
	            // 创建一个子目录节点
	            zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(),
	              Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
	            System.out.println("1:"+new String(zk.getData("/testRootPath",false,null)));
	            // 取出子目录节点列表
	            System.out.println("2:"+zk.getChildren("/testRootPath",true));
	            // 修改子目录节点数据
	            zk.setData("/testRootPath/testChildPathOne","modifyChildDataOne".getBytes(),-1);
	            System.out.println("3:"+"目录节点状态：["+zk.exists("/testRootPath",true)+"]");
	            // 创建另外一个子目录节点
	            zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(),
	              Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
	            System.out.println("4:"+new String(zk.getData("/testRootPath/testChildPathTwo",true,null)));
	            // 删除子目录节点
	            zk.delete("/testRootPath/testChildPathTwo",-1);
	            zk.delete("/testRootPath/testChildPathOne",-1);
	            // 删除父目录节点
	            zk.delete("/testRootPath",-1);
	            // 关闭连接
	            zk.close();

         /**
          * {<a href='http://www.tuicool.com/articles/77FJzuj'>@link</a>}
          * 在创建znode时可以设置该znode的ACL列表。接口org.apache.zookeeper.ZooDefs.Ids中有一些已经设置好的权限常量，例如：

          (1)、 OPEN_ACL_UNSAFE ：完全开放。 事实上这里是采用了world验证模式，由于每个zk连接都有world验证模式，所以znode在设置了 OPEN_ACL_UNSAFE 时，是对所有的连接开放。

          (2)、 CREATOR_ALL_ACL ：给创建该znode连接所有权限。 事实上这里是采用了auth验证模式，使用sessionID做验证。所以设置了 CREATOR_ALL_ACL 时，创建该znode的连接可以对该znode做任何修改。

          (3)、 READ_ACL_UNSAFE ：所有的客户端都可读。 事实上这里是采用了world验证模式，由于每个zk连接都有world验证模式，所以znode在设置了READ_ACL_UNSAFE时，所有的连接都可以读该znode。
          */
     }
}
