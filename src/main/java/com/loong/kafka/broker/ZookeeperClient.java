package com.loong.kafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ZooKeeper链接客户端
 * <p>
 * 【ZooKeeper简介】
 * ZooKeeper 是一个分布式协调服务，用于管理集群中的节点、服务注册、选主等场景。
 * 想象它是一个共享的"信息板"，所有服务都可以在上面写入和读取信息。
 * <p>
 * 【在我们的Kafka中的作用】
 * 1. 服务注册：broker启动时在ZooKeeper上注册自己的信息
 * 2. 元数据管理：管理topic、partition的归属信息
 * 3. 选主控制：选举controller节点（负责管理整个集群）
 * <p>
 * 【核心概念】
 * - 节点(ZNode)：ZooKeeper中的数据存储单元，类似于文件系统中的文件
 * - 临时节点(Ephemeral)：当客户端断开连接时自动删除，适用于服务注册
 * - 持久节点(Persistent)：手动删除前一直存在，适用于保存配置信息
 * - Watch机制：监听节点变化，变化时会收到通知
 */
public class ZookeeperClient implements Watcher {

    public static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());

    /**
     * Session超时时间（毫秒）
     * 如果超过这个时间ZooKeeper服务端没有收到客户端的心跳，session会失效
     * 30秒是一个比较合理的值，既不会太频繁，也不会太长
     */
    private static final int SESSION_TIMEOUT = 30000;

    private final String host;          // ZooKeeper服务端主机名
    private final int port;             // ZooKeeper服务端端口
    private ZooKeeper zooKeeper;        // ZooKeeper客户端实例
    private CountDownLatch connectedSignal = new CountDownLatch(1); // 连接等待锁

    /**
     * 构造函数：指定ZooKeeper服务端的地址
     * @param host ZooKeeper所在服务器的主机名或IP地址
     * @param port ZooKeeper服务端口，默认为2181
     */
    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * 连接到ZooKeeper服务器
     * <p>
     * 【连接流程】
     * 1. 创建ZooKeeper客户端实例，指定服务端地址、超时时间、 watcher
     * 2. 等待连接成功（通过CountDownLatch实现同步等待）
     * 3. 创建Kafka运行时所需的路径（如果不存在）
     * <p>
     * 【注意】
     * 这是一个阻塞方法，只有连接成功或异常才会返回
     *
     * @throws IOException 如果创建ZooKeeper客户端失败
     * @throws InterruptedException 如果等待连接过程中被中断
     */
    public void connect() throws IOException, InterruptedException {
        // 创建ZooKeeper客户端
        // 参数1：连接字符串，格式为 "host:port"
        // 参数2：session超时时间
        // 参数3：watcher实例，用于接收连接状态变更事件
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);

        // 阻塞等待连接成功
        // 当连接成功时，process()方法会被调用，countDown()会解除阻塞
        connectedSignal.await();

        // 创建Kafka运行时必需的ZK节点路径
        createPath("/brokers");   // 存储broker注册信息
        createPath("/topics");    // 存储topic配置信息
        createPath("/controller"); // 存储controller选举信息
    }

    /**
     * 获取ZooKeeper连接字符串
     * @return 格式为 "host:port" 的连接字符串
     */
    public String getConnectString() {
        return host + ":" + port;
    }

    /**
     * 关闭ZooKeeper连接
     * 【重要】使用完必须调用此方法释放资源
     *
     * @throws InterruptedException 如果关闭过程中被中断
     */
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    /**
     * 创建或更新持久节点
     * <p>
     * 【持久节点特点】
     * - 创建后一直存在，不会因客户端断开而删除
     * - 数据会被持久化存储
     * - 适用于存储配置信息等需要持久保存的数据
     * <p>
     * 【业务逻辑】
     * 如果节点不存在，则创建；如果已存在，则更新数据
     *
     * @param path 节点路径，如 "/brokers/0"
     * @param data 节点数据，如broker的注册信息 JSON字符串
     * @throws KeeperException 如果ZooKeeper操作失败（如权限不足、节点已存在等）
     * @throws InterruptedException 如果操作被中断
     */
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        // 先检查节点是否已存在
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            // 节点不存在，创建新节点
            // 参数1：路径  参数2：数据  参数3：ACL权限  参数4：节点类型（PERSISTENT=持久）
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            // 节点已存在，更新数据
            // -1表示不基于任何版本进行更新（乐观锁的最后一个参数）
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    /**
     * 创建临时节点
     * <p>
     * 【临时节点特点】
     * - 当客户端断开连接时，节点会自动被删除
     * - 常用于服务注册和心跳检测
     * - 如果客户端崩溃，ZooKeeper会自动清理其创建的临时节点
     * <p>
     * 【典型用途】
     * broker启动时创建临时节点表示自己在线，如果broker崩溃，
     * 连接断开后临时节点自动消失，其他节点就知道该broker不可用了
     *
     * @param path 节点路径
     * @param data 节点数据
     * @return true 如果创建成功，false 如果节点已存在
     * @throws KeeperException 如果ZooKeeper操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            // 创建临时节点
            // CreateMode.EPHEMERAL 确保客户端断开时节点自动删除
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        } else {
            LOGGER.info("Ephemeral node already exists: " + path);
            return false;
        }
    }

    /**
     * 检查节点是否存在
     * @param path 节点路径
     * @return true 存在，false 不存在
     * @throws KeeperException 如果ZooKeeper操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public boolean exists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        return stat != null;
    }

    /**
     * 获取节点数据
     * @param path 节点路径
     * @return 节点中存储的数据（字符串形式）
     * @throws KeeperException 如果节点不存在或操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }

    /**
     * 设置节点数据（更新操作）
     * @param path 节点路径
     * @param data 新的数据内容
     * @throws KeeperException 如果节点不存在或操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
    }

    /**
     * 获取节点的所有子节点列表
     * @param path 父节点路径
     * @return 子节点名称列表，如果父节点不存在则返回空列表
     * @throws KeeperException 如果操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            // 如果节点不存在，返回空列表而不是抛出异常
            return new ArrayList<>();
        }
    }

    /**
     * 递归创建路径（如果不存在）
     * <p>
     * 【工作原理】
     * 这是一个递归方法，从根路径开始逐级创建。
     * 例如创建 "/brokers/topics/test"：
     * 1. 先检查并创建根 "/"（但实际上 "/" 已存在，会跳过）
     * 2. 再检查并创建 "/brokers"
     * 3. 再检查并创建 "/brokers/topics"
     * 4. 最后创建 "/brokers/topics/test"
     * <p>
     * 【为什么需要这个方法】
     * ZooKeeper要求创建子节点时父节点必须存在，
     * 但创建父节点时又要求其父节点存在...所以需要递归创建
     *
     * @param path 要创建的完整路径
     */
    private void createPath(String path) {
        try {
            // 根路径直接返回，不做任何操作
            if (path.equals("/")) {
                return;
            }

            // 找到父路径
            int lastSlashIndex = path.lastIndexOf('/');
            if (lastSlashIndex > 0) {
                String parentPath = path.substring(0, lastSlashIndex);
                // 递归创建父路径（先确保父路径存在）
                createPath(parentPath);
            }

            // 如果当前路径不存在，则创建
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Created ZooKeeper path: " + path);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create path: " + path, e);
        }
    }

    /**
     * 监听子节点列表变化
     * <p>
     * 【Watch机制简介】
     * Watch是ZooKeeper的核心功能，允许客户端订阅节点变化通知。
     * 当被监听的节点发生以下变化时会收到通知：
     * - 子节点添加
     * - 子节点删除
     * - 子节点数据更新
     * <p>
     * 【典型使用场景】
     * 监听 "/brokers" 节点，当新的broker注册时，立即知道有新的broker加入集群
     * <p>
     * 【重要特性】
     * Watch是一次性的！收到一次通知后就会失效，需要重新注册才能继续监听
     * 本方法内部已经实现了自动重新注册（递归调用watchChildren）
     *
     * @param path 要监听的节点路径
     * @param callback 变化发生时的回调函数
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            // 获取子节点列表并设置监听
            // 第二个参数是一个Watcher，当子节点变化时会触发
            List<String> children = zooKeeper.getChildren(path, event -> {
                // 只有子节点列表变化才触发（不是数据变化）
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        // 重新获取新列表，并继续监听（实现持续监听）
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                // 再次变化时递归处理
                                watchChildren(path, callback);
                            }
                        });
                        // 通知回调
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error processing children changed event", e);
                    }
                }
            });
            // 首次调用也触发回调（返回当前子节点列表）
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    /**
     * 监听单个节点的变化
     * <p>
     * 【可监听的事件类型】
     * - NodeCreated：节点被创建
     * - NodeDeleted：节点被删除
     * - NodeDataChanged：节点数据更新
     * <p>
     * 【典型使用场景】
     * 监听某个broker的状态节点，当该broker异常时节点被删除，
     * 我们立即收到通知并更新集群状态
     *
     * @param path 要监听的节点路径
     * @param callback 节点变化时的回调函数
     */
    public void watchNode(String path, NodeCallback callback) {
        try {
            // exists方法会设置一个Watch到指定节点
            zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    // 节点被删除
                    callback.onNodeChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    // 节点数据更新
                    callback.onNodeChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                    // 节点被创建
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    /**
     * 删除节点
     * 【注意】如果节点有子节点，需要先删除子节点才能删除父节点
     *
     * @param path 要删除的节点路径
     * @throws KeeperException 如果节点不存在或删除失败
     * @throws InterruptedException 如果操作被中断
     */
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        if (exists(path)) {
            zooKeeper.delete(path, -1);
            LOGGER.info("Deleted node: " + path);
        }
    }


    /**
     * ZooKeeper事件处理回调
     * <p>
     * 【重要】这个方法是Watcher接口的实现
     * 所有连接状态变更事件都会回调到这里
     * <p>
     * 【事件类型】
     * - SyncConnected：成功连接到ZooKeeper
     * - Disconnected：断开连接（不一定是错误，可能只是网络抖动）
     * - Expired：Session过期（超时未心跳或ZooKeeper服务端重启）
     *
     * @param event ZooKeeper事件
     */
    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            // 连接成功，解除主线程的等待阻塞
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            // 断开连接
            // 这种情况通常是网络问题，不建议立即重连，因为ZooKeeper会自动重试
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            // Session过期，必须重新创建连接
            // 过期原因通常是：客户端长时间无响应或服务端重启
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close(); // 关闭旧连接
                }
                // 重置信号量，创建新连接
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    /**
     * 子节点变化回调接口
     * 用于 watchChildren 方法，当监听的节点子列表发生变化时调用
     */
    public interface ChildrenCallback {
        /**
         * 子节点列表变化时的回调
         * @param children 最新的子节点名称列表
         */
        void onChildrenChanged(List<String> children);
    }

    /**
     * 节点变化回调接口
     * 用于 watchNode 方法，当监听的节点创建/删除/数据变化时调用
     */
    public interface NodeCallback {
        /**
         * 节点变化时的回调
         */
        void onNodeChanged();
    }
}