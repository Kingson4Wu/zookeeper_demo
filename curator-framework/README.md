按照官方给出的文档和包结构，可以看出Curator功能分两大类，一是对zookeeper的一些基本命令的封装，比如增删改查。是他的framework模块，一个是他的高级特性，即recipes模块。
<http://blog.csdn.net/cherry_sun/article/details/7916110>
<http://supben.iteye.com/blog/2094077>


Curator框架提供了一种流式接口。 操作通过builder串联起来， 这样方法调用类似语句一样。

client.create().forPath("/head", new byte[0]);
client.delete().inBackground().forPath("/head");
client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/head/child", new byte[0]);
client.getData().watched().inBackground().forPath("/test");
CuratorFramework提供的方法：

方法名	描述
create()	开始创建操作， 可以调用额外的方法(比如方式mode 或者后台执行background) 并在最后调用forPath()指定要操作的ZNode
delete()	开始删除操作. 可以调用额外的方法(版本或者后台处理version or background)并在最后调用forPath()指定要操作的ZNode
checkExists()	开始检查ZNode是否存在的操作. 可以调用额外的方法(监控或者后台处理)并在最后调用forPath()指定要操作的ZNode
getData()	开始获得ZNode节点数据的操作. 可以调用额外的方法(监控、后台处理或者获取状态watch, background or get stat) 并在最后调用forPath()指定要操作的ZNode
setData()	开始设置ZNode节点数据的操作. 可以调用额外的方法(版本或者后台处理) 并在最后调用forPath()指定要操作的ZNode
getChildren()	开始获得ZNode的子节点列表。 以调用额外的方法(监控、后台处理或者获取状态watch, background or get stat) 并在最后调用forPath()指定要操作的ZNode
inTransaction()	开始是原子ZooKeeper事务. 可以复合create, setData, check, and/or delete 等操作然后调用commit()作为一个原子操作提交
后台操作的通知和监控可以通过ClientListener接口发布. 你可以在CuratorFramework实例上通过addListener()注册listener, Listener实现了下面的方法:

eventReceived() 一个后台操作完成或者一个监控被触发
事件类型以及事件的方法如下：

Event Type	Event Methods
CREATE	getResultCode() and getPath()
DELETE	getResultCode() and getPath()
EXISTS	getResultCode(), getPath() and getStat()
GETDATA	getResultCode(), getPath(), getStat() and getData()
SETDATA	getResultCode(), getPath() and getStat()
CHILDREN	getResultCode(), getPath(), getStat(), getChildren()
WATCHED	getWatchedEvent()
还可以通过ConnectionStateListener接口监控连接的状态。 强烈推荐你增加这个监控器。

你可以使用命名空间Namespace避免多个应用的节点的名称冲突。 CuratorFramework提供了命名空间的概念，这样CuratorFramework会为它的API调用的path加上命名空间：

CuratorFramework    client = CuratorFrameworkFactory.builder().namespace("MyApp") ... build();
 ...
client.create().forPath("/test", data);
// node was actually written to: "/MyApp/test"
Curator还提供了临时的CuratorFramework： CuratorTempFramework， 一定时间不活动后连接会被关闭。这hi基于Camille Fournier的一篇文章： http://whilefalse.blogspot.com/2012/12/building-global-highly-available.html.

创建builder时不是调用build()而是调用buildTemp()。 3分钟不活动连接就被关闭，你也可以指定不活动的时间。 它只提供了下面几个方法：

    public void     close();
    public CuratorTransaction inTransaction() throws Exception;
    public TempGetDataBuilder getData() throws Exception;