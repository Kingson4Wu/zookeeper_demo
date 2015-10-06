package com.kxw.zookeeper.single;

/**
 * ZooKeeperOperator.java
 * 版权所有(C) 2013 
 * 创建:cuiran 2013-01-16 15:03:40
 */

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * TODO
 * @author cuiran
 * @version TODO
 */
public class ZooKeeperOperator extends AbstractZooKeeper {
	
	private static Log log = LogFactory.getLog(ZooKeeperOperator.class.getName());

	/**
	 * 
	 *<b>function:</b>创建持久态的znode,比支持多层创建.比如在创建/parent/child的情况下,无/parent.无法通过
	 *@author cuiran
	 *@createDate 2013-01-16 15:08:38
	 *@param path
	 *@param data
	 *@throws KeeperException
	 *@throws InterruptedException
	 */
	public void create(String path,byte[] data)throws KeeperException, InterruptedException{
		/**
		 * 此处采用的是CreateMode是PERSISTENT  表示The znode will not be automatically deleted upon client's disconnect.
		 * EPHEMERAL 表示The znode will be deleted upon the client's disconnect.
		 */ 
		this.zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	public void delete(String string) throws InterruptedException, KeeperException {
		this.zooKeeper.delete(string, -1);
		
	}
	
	
	/**
	 * 
	 *<b>function:</b>获取节点信息
	 *@author cuiran
	 *@createDate 2013-01-16 15:17:22
	 *@param path
	 *@throws KeeperException
	 *@throws InterruptedException
	 */
	public void getChild(String path) throws KeeperException, InterruptedException{   
		try{
			List<String> list=this.zooKeeper.getChildren(path, false);
			if(list.isEmpty()){
				log.debug(path+"中没有节点");
			}else{
				log.debug(path+"中存在节点");
				for(String child:list){
					log.debug("节点为："+child);
				}
			}
		}catch (KeeperException.NoNodeException e) {
			// TODO: handle exception
			 throw e;   

		}
	}
	
	public byte[] getData(String path) throws KeeperException, InterruptedException {   
        return  this.zooKeeper.getData(path, false,null);   
    }  
	
	 public static void main(String[] args) {
		 try {   
			 //1.
	            ZooKeeperOperator zkoperator             = new ZooKeeperOperator();   
	            zkoperator.connect("127.0.0.1");
	            
	            byte[] data = new byte[]{'a','b','c','d'};   
	               
	           zkoperator.create("/root",null);   
	            System.out.println(Arrays.toString(zkoperator.getData("/root")));   
               
            zkoperator.create("/root/child1",data);   
	            System.out.println(Arrays.toString(zkoperator.getData("/root/child1")));   
               
            zkoperator.create("/root/child2",data);   
            System.out.println(Arrays.toString(zkoperator.getData("/root/child2")));   
	               
	            String zktest="ZooKeeper的Java API测试";
	            zkoperator.create("/root/child3", zktest.getBytes());
	            log.debug("获取设置的信息："+new String(zkoperator.getData("/root/child3")));
	            
	            System.out.println("节点孩子信息:");   
	            zkoperator.getChild("/root");   
	             
	            zkoperator.delete("/root/child3");
	            zkoperator.delete("/root/child2"); 
	            zkoperator.delete("/root/child1"); 
	            zkoperator.delete("/root"); 
	            
	            zkoperator.close();   
	            
	            //-------------------
			 
	           //2. 
	         // 创建一个与服务器的连接
	        /*    ZooKeeper zk = new ZooKeeper("localhost:" + 2181, 
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
	            */
	            
	        } catch (Exception e) {   
	            e.printStackTrace();   
	        }   

	}
	
}
