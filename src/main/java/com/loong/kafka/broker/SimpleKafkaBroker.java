package com.loong.kafka.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SimpleKafka 消息代理（Broker）的核心实现类。
 *
 * <h3>什么是 Broker？</h3>
 * Broker 是 Kafka 集群中的"服务器节点"，负责：
 * <ul>
 *   <li><b>存储消息</b>：将生产者发来的消息持久化到磁盘分区中</li>
 *   <li><b>响应消费</b>：根据消费者的请求，从分区中读取并返回消息</li>
 *   <li><b>集群协同</b>：通过 ZooKeeper 与其他 Broker 协作，选举控制器、同步元数据</li>
 *   <li><b>数据复制</b>：作为 Leader 时，将消息复制给 Follower 副本</li>
 * </ul>
 *
 * <h3>核心概念速览</h3>
 * <ul>
 *   <li><b>Topic（主题）</b>：消息的分类标签，类似数据库中的"表"</li>
 *   <li><b>Partition（分区）</b>：Topic 的物理分片，每个分区是一个有序、不可变的消息序列</li>
 *   <li><b>Leader / Follower</b>：每个分区有一个 Leader 负责读写，多个 Follower 作为备份</li>
 *   <li><b>Controller（控制器）</b>：集群中唯一的"大管家"Broker，负责分区分配、Leader 选举等管理工作</li>
 *   <li><b>ZooKeeper</b>：分布式协调服务，存储集群元数据（有哪些 Broker、有哪些 Topic 等）</li>
 * </ul>
 *
 * <h3>启动流程</h3>
 * <ol>
 *   <li>创建数据目录，初始化 ZooKeeper 客户端</li>
 *   <li>绑定网络端口，开始监听客户端连接</li>
 *   <li>向 ZooKeeper 注册自身（临时节点，断开即消失）</li>
 *   <li>参与 Controller 选举</li>
 *   <li>从 ZooKeeper 加载已有的 Topic 和 Partition 信息</li>
 * </ol>
 *
 * @see Partition       分区实现
 * @see ZookeeperClient ZooKeeper 客户端
 * @see BrokerInfo      集群中其他 Broker 的信息
 * @see Protocol        自定义网络协议常量
 */
public class SimpleKafkaBroker {

    /** 日志记录器，用于输出运行时的调试和错误信息 */
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());

    /** 数据文件在磁盘上的根目录名称 */
    private static final String DATA_DIR = "data";

    // ======================== Broker 基础属性 ========================

    /** 当前 Broker 在集群中的唯一数字编号 */
    private final int brokerId;

    /** 当前 Broker 绑定的主机名或 IP 地址 */
    private final String brokerHost;

    /** 当前 Broker 监听的网络端口号 */
    private final int brokerPort;

    // ======================== 消息存储 ========================

    /**
     * 当前 Broker 管理的所有 Topic 及其分区。
     * <p>
     * 数据结构：Map<主题名称, List<该主题下的所有分区>>
     * <p>
     * 使用 ConcurrentHashMap 保证多线程安全——网络线程和复制线程会同时访问这个 Map。
     */
    private final Map<String, List<Partition>> topics;

    // ======================== 线程管理 ========================

    /**
     * 线程池，用于并发处理客户端连接、消息复制等任务。
     * 固定大小为 10 个线程，避免无限制创建线程导致资源耗尽。
     */
    private final ExecutorService executor;

    // ======================== 网络通信 ========================

    /**
     * 服务端 Socket 通道，用于监听和接受客户端的 TCP 连接。
     * 采用 Java NIO（非阻塞 IO），一个线程可以管理多个连接。
     */
    private final ServerSocketChannel serverChannel;

    // ======================== 状态标识 ========================

    /**
     * Broker 是否正在运行。
     * 使用 AtomicBoolean 保证多线程环境下的可见性和原子性——启动时设为 true，关闭时设为 false。
     */
    private final AtomicBoolean isRunning;

    /**
     * 当前 Broker 是否是集群的 Controller（控制器）。
     * 整个集群同一时刻只有一个 Controller，通过 ZooKeeper 选举产生。
     */
    private final AtomicBoolean isController;

    // ======================== 集群元数据 ========================

    /**
     * 集群中所有 Broker 的信息缓存。
     * <p>
     * Key 是 Broker ID，Value 是包含主机、端口等信息的 BrokerInfo 对象。
     * 当 ZooKeeper 通知 Broker 列表变化时会更新此 Map。
     */
    private final Map<Integer, BrokerInfo> clusterMetadata;

    /**
     * ZooKeeper 客户端，负责与 ZooKeeper 服务端通信。
     * 用途包括：注册 Broker、参与选举、存储和读取 Topic/Partition 元数据。
     */
    private final ZookeeperClient zkClient;

    /** ZooKeeper 服务端的主机地址（默认本机） */
    public final String ZOOKEEPER_HOST = "127.0.0.1";

    // ======================== 构造方法 ========================

    /**
     * 创建一个新的 Broker 实例。
     * <p>
     * 构造时完成以下初始化工作：
     * <ol>
     *   <li>保存 Broker ID、主机地址和端口号</li>
     *   <li>创建线程安全的 Topic 存储容器</li>
     *   <li>创建 10 个线程的固定大小线程池</li>
     *   <li>打开服务端 Socket 通道（还未绑定端口，需调用 start() 才会绑定）</li>
     *   <li>初始化运行状态为 false，控制器状态为 false</li>
     *   <li>在磁盘上创建数据目录 {@code data/<brokerId>/}</li>
     *   <li>创建 ZooKeeper 客户端实例</li>
     * </ol>
     *
     * @param brokerId Broker 的唯一数字编号，例如 0, 1, 2
     * @param host     绑定的主机名或 IP 地址
     * @param port     监听的 TCP 端口号
     * @param zkPort   ZooKeeper 服务端口号（默认 2181）
     * @throws IOException 如果创建 Socket 通道或目录失败
     */
    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();

        // 确保当前 Broker 的数据目录存在，例如 data/0/
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        this.zkClient = new ZookeeperClient(ZOOKEEPER_HOST, zkPort);
    }

    // ======================== 启动与停止 ========================

    /**
     * 启动 Broker 服务。
     * <p>
     * 启动流程按顺序执行：
     * <ol>
     *   <li>使用 CAS 操作将运行状态从 false 切换为 true，确保只启动一次</li>
     *   <li>将 ServerSocket 绑定到指定的主机和端口</li>
     *   <li>设为非阻塞模式（NIO），一个线程可同时处理多个连接</li>
     *   <li>向 ZooKeeper 注册当前 Broker（创建临时节点）</li>
     *   <li>参与 Controller 选举：最先创建 /controller 节点的 Broker 成为控制器</li>
     *   <li>从 ZooKeeper 加载集群已有的 Topic 和分区信息</li>
     *   <li>提交"接受客户端连接"任务到线程池，开始服务</li>
     * </ol>
     *
     * @throws IOException 如果端口绑定失败
     */
    public void start() throws IOException {
        if (isRunning.compareAndSet(false, true)) {
            // 绑定网络端口，开始监听
            serverChannel.socket().bind(new InetSocketAddress(brokerHost, brokerPort));
            serverChannel.configureBlocking(false);

            LOGGER.info("SimpleKafka broker started on " + brokerHost + ":" + brokerPort);

            // 向 ZooKeeper 注册自己
            registerWithZookeeper();

            // 参与 Controller 选举
            electController();

            // 加载集群已有的 Topic
            loadTopics();

            // 在后台线程中循环接受客户端连接
            executor.submit(this::acceptConnections);
        }
    }

    /**
     * 优雅地停止 Broker 服务。
     * <p>
     * 停止流程：
     * <ol>
     *   <li>关闭 ServerSocket，不再接受新连接</li>
     *   <li>关闭所有 Partition（刷新缓冲区、释放文件句柄）</li>
     *   <li>关闭线程池，等待最多 5 秒让现有任务完成</li>
     *   <li>关闭 ZooKeeper 连接（临时节点会自动删除，通知集群此 Broker 下线）</li>
     * </ol>
     * <p>
     * 该方法被注册为 JVM 关闭钩子（Shutdown Hook），在 Ctrl+C 或进程终止时自动调用。
     */
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping SimpleKafka broker...");

                // 关闭服务端 Socket，停止接受新连接
                serverChannel.close();

                // 关闭所有分区的文件资源
                for (List<Partition> partitions : topics.values()) {
                    for (Partition partition : partitions) {
                        partition.close();
                    }
                }

                // 关闭线程池
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);

                // 关闭 ZooKeeper 连接，临时节点自动删除
                zkClient.close();

                LOGGER.info("SimpleKafka broker stopped");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error stopping broker", e);
            }
        }
    }

    // ======================== ZooKeeper 注册与集群感知 ========================

    /**
     * 向 ZooKeeper 注册当前 Broker。
     * <p>
     * 具体步骤：
     * <ol>
     *   <li>连接 ZooKeeper 服务端</li>
     *   <li>创建临时节点 {@code /brokers/<brokerId>}，存储 "host:port" 格式的地址信息。</li>
     *   <li>将自身信息加入本地集群元数据缓存</li>
     *   <li>监听 {@code /brokers} 节点的子节点变化——当有 Broker 加入或离开时会收到通知，触发 {@link #onBrokersChanged}</li>
     * </ol>
     * <p>
     * <b>为什么用临时节点？</b>临时节点的生命周期与会话绑定——Broker 崩溃或网络断开后，
     * ZooKeeper 会自动删除该节点，其他 Broker 会收到通知，从而感知到集群成员变化。
     */
    private void registerWithZookeeper() {
        try {
            zkClient.connect();
            String brokerPath = "/brokers/" + brokerId;
            String brokerData = brokerHost + ":" + brokerPort;
            zkClient.createEphemeralNode(brokerPath, brokerData);

            // 加入本地缓存
            BrokerInfo selfInfo = new BrokerInfo(brokerId, brokerHost, brokerPort);
            clusterMetadata.put(brokerId, selfInfo);

            // 监听集群 Broker 列表变化
            zkClient.watchChildren("/brokers", this::onBrokersChanged);

            LOGGER.info("Registered with ZooKeeper at " + zkClient.getConnectString());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }

    /**
     * ZooKeeper 通知 /brokers 子节点发生变化时的回调。
     * <p>
     * 当集群中有新 Broker 加入、或已有 Broker 离开（包括崩溃）时触发。
     * 处理逻辑：
     * <ol>
     *   <li>遍历 ZooKeeper 返回的最新 Broker ID 列表</li>
     *   <li>不认识的新 Broker → 从 ZooKeeper 读取其地址信息，加入本地缓存</li>
     *   <li>已不在列表中的旧 Broker → 从本地缓存中移除</li>
     *   <li>如果当前 Broker 是 Controller → 执行分区重分配（新 Broker 可以分担负载）</li>
     *   <li>如果不是 Controller → 重新尝试参与 Controller 选举（可能之前的 Controller 下线了）</li>
     * </ol>
     *
     * @param brokerIds ZooKeeper 中当前存在的所有 Broker ID 列表
     */
    private void onBrokersChanged(List<String> brokerIds) {
        LOGGER.info("Broker change detected. Current brokers: " + brokerIds);

        // 处理新加入的 Broker
        for (String id : brokerIds) {
            try {
                int brokerId = Integer.parseInt(id);
                if (!clusterMetadata.containsKey(brokerId)) {
                    // 从 ZooKeeper 读取该 Broker 的地址信息
                    String brokerData = zkClient.getData("/brokers/" + id);
                    String[] hostPort = brokerData.split(":");
                    BrokerInfo info = new BrokerInfo(
                            brokerId,
                            hostPort[0],
                            Integer.parseInt(hostPort[1]));
                    clusterMetadata.put(brokerId, info);
                    LOGGER.info("Added broker: " + info);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to process broker info", e);
            }
        }

        // 移除已离开的 Broker
        List<Integer> toRemove = new ArrayList<>();
        for (Integer brokerId : clusterMetadata.keySet()) {
            if (!brokerIds.contains(String.valueOf(brokerId))) {
                toRemove.add(brokerId);
            }
        }

        for (Integer brokerId : toRemove) {
            clusterMetadata.remove(brokerId);
            LOGGER.info("Removed broker: " + brokerId);
        }

        // 根据自身角色做出反应
        if (!brokerIds.contains(String.valueOf(brokerId)) && isController.get()) {
            // 自身不在集群中（理论上不应出现，但做防御性处理）
            isController.set(false);
            LOGGER.info("This broker is no longer in the cluster, giving up controller status");
        } else if (isController.get()) {
            // 作为 Controller，集群变化后需要重新分配分区
            rebalancePartitions();
        } else {
            // 不是 Controller，重新尝试选举
            electController();
        }
    }

    // ======================== Controller 选举 ========================

    /**
     * 参与 Controller（控制器）选举。
     *
     * <h4>选举机制（简化版）</h4>
     * 利用 ZooKeeper 的"临时节点只能创建一次"特性实现分布式互斥锁：
     * <ol>
     *   <li>尝试在 ZooKeeper 创建临时节点 {@code /controller}，数据为当前 Broker ID</li>
     *   <li>创建成功 → 当前 Broker 成为 Controller</li>
     *   <li>创建失败（节点已存在）→ 读取节点数据获取当前 Controller ID，监听该节点等待其下线</li>
     * </ol>
     *
     * <h4>Controller 的职责</h4>
     * <ul>
     *   <li>创建 Topic 时，决定每个分区的 Leader 和 Follower 分配</li>
     *   <li>Broker 下线时，重新分配受影响的分区</li>
     *   <li>通知其他 Broker 加载新 Topic</li>
     * </ul>
     */
    private void electController() {
        try {
            String controllerPath = "/controller";

            // 检查节点是否已存在
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                // 处理脏数据：节点存在但内容为空
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                    LOGGER.info("Deleted empty controller node");
                }
            }

            boolean becameController = false;
            if (!nodeExists) {
                // 尝试抢占 Controller 位置
                becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
            }

            if (becameController) {
                // 选举成功！
                isController.set(true);
                LOGGER.info("This broker is now the active controller");

                // 作为新 Controller，立即执行一次分区再平衡
                rebalancePartitions();
            } else {
                // 选举失败，读取当前 Controller 是谁
                String controllerId = zkClient.getData(controllerPath);
                if (controllerId == null || controllerId.trim().isEmpty()) {
                    // 异常情况：节点存在但无数据，1 秒后重试
                    LOGGER.warning("Controller node exists but has no data. This is unexpected.");
                    new Thread(() -> {
                        try {
                            Thread.sleep(1000);
                            electController();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }).start();
                    return;
                }

                LOGGER.info("Current controller is broker " + controllerId);

                // 监听 Controller 节点，当它消失时重新选举
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);

            // 选举异常，2 秒后重试
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    /**
     * Controller 节点变化时的回调。
     * 当 /controller 节点被删除（Controller 下线）或数据变更时触发，立即发起新一轮选举。
     */
    private void onControllerChange() {
        LOGGER.info("Controller changed, initiating new election");
        electController();
    }

    // ======================== 分区再平衡 ========================

    /**
     * 重新分配集群中所有分区的 Leader 和 Follower。
     * <p>
     * <b>只有 Controller 才能调用此方法。</b>非 Controller 直接返回。
     * <p>
     * 再平衡逻辑：
     * <ol>
     *   <li>遍历当前 Broker 已知的所有 Topic 和 Partition</li>
     *   <li>检查每个分区的 Leader：如果 Leader 不存在或已从集群中消失，为其分配一个新 Leader</li>
     *   <li>同时重新分配 Follower 列表（最多 3 个副本）</li>
     *   <li>将新的 Leader/Follower 分配写入 ZooKeeper，持久化元数据</li>
     * </ol>
     */
    private void rebalancePartitions() {
        if (!isController.get()) {
            return;
        }

        LOGGER.info("Rebalancing partitions across cluster");

        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            for (Partition partition : partitions) {
                // 检查 Leader 是否有效（-1 表示无 Leader，或 Leader Broker 已下线）
                if (partition.getLeader() == -1 || !clusterMetadata.containsKey(partition.getLeader())) {
                    List<Integer> brokers = new ArrayList<>(clusterMetadata.keySet());
                    if (!brokers.isEmpty()) {
                        // 选择第一个可用 Broker 作为新 Leader
                        int newLeader = brokers.get(0);
                        partition.setLeader(newLeader);

                        // 分配 Follower（从剩余 Broker 中选择，最多 3 个副本包括 Leader）
                        List<Integer> followers = new ArrayList<>();
                        for (int i = 1; i < Math.min(brokers.size(), 3); i++) {
                            followers.add(brokers.get(i));
                        }
                        partition.setFollowers(followers);

                        // 将新的分配方案持久化到 ZooKeeper
                        updatePartitionMetadata(topic, partition);

                        LOGGER.info("Reassigned partition " + partition.getId() +
                                " of topic " + topic +
                                " to leader " + newLeader +
                                " with followers " + followers);
                    }
                }
            }
        }
    }

    /**
     * 将分区的 Leader/Follower 分配信息写入 ZooKeeper，实现元数据持久化。
     * <p>
     * 存储格式：{@code "leaderId;follower1,follower2,follower3,"}
     * <p>
     * 如果 ZooKeeper 中该分区节点不存在则创建，已存在则更新。
     *
     * @param topic     主题名称
     * @param partition 分区对象（从中获取 Leader 和 Follower 列表）
     */
    private void updatePartitionMetadata(String topic, Partition partition) {
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            // 构建存储字符串："leader;follower1,follower2,..."
            String data = partition.getLeader() + ";";
            for (int follower : partition.getFollowers()) {
                data += follower + ",";
            }

            if (zkClient.exists(path)) {
                zkClient.setData(path, data);
            } else {
                zkClient.createPersistentNode(path, data);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update partition metadata", e);
        }
    }

    // ======================== Topic 加载 ========================

    /**
     * 从 ZooKeeper 批量加载所有 Topic 及其分区信息。
     * <p>
     * 通常在 Broker 启动时调用一次，将 ZooKeeper 中已有的 Topic 全部加载到内存。
     * 加载失败不会中断整体流程——每个 Topic 的错误单独记录，继续处理下一个。
     */
    public void loadTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");

            for (String topic : topicNames) {
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);
                }
            }

            LOGGER.info("Loaded " + topics.size() + " topics");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topics", e);
        }
    }

    /**
     * 从 ZooKeeper 加载单个 Topic 的完整信息到内存。
     * <p>
     * 加载过程：
     * <ol>
     *   <li>检查 ZooKeeper 中 {@code /topics/<topicName>} 节点是否存在</li>
     *   <li>在本地磁盘创建 Topic 的数据目录</li>
     *   <li>读取 {@code /topics/<topicName>/partitions/} 下的所有子节点（即所有分区 ID）</li>
     *   <li>对每个分区，解析其 Leader 和 Follower 信息，创建 Partition 对象</li>
     *   <li>将完整的分区列表放入内存 Map</li>
     * </ol>
     * <p>
     * ZooKeeper 中分区数据的存储格式：{@code "leaderId;follower1,follower2,..."}
     * <p>
     * 如果 Topic 已加载过（在内存中存在），则跳过，避免重复加载。
     *
     * @param topic 要加载的主题名称
     * @throws Exception 如果 ZooKeeper 中不存在该 Topic
     */
    private void loadTopic(String topic) throws Exception {
        if (topics.containsKey(topic)) {
            LOGGER.info("Topic already loaded: " + topic);
            return;
        }

        String topicPath = "/topics/" + topic;
        if (!zkClient.exists(topicPath)) {
            throw new Exception("Topic does not exist in ZooKeeper: " + topic);
        }

        // 创建本地数据目录
        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        // 获取该 Topic 下的所有分区 ID
        List<String> partitionIds = zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = new ArrayList<>();

        for (String partitionId : partitionIds) {
            int id = Integer.parseInt(partitionId);
            String partitionPath = topicPath + "/partitions/" + partitionId;
            String partitionData = zkClient.getData(partitionPath);

            // 解析分区元数据："leader;follower1,follower2,..."
            String[] parts = partitionData.split(";");
            int leader = Integer.parseInt(parts[0]);

            List<Integer> followers = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty()) {
                String[] followerIds = parts[1].split(",");
                for (String followerId : followerIds) {
                    if (!followerId.isEmpty()) {
                        followers.add(Integer.parseInt(followerId));
                    }
                }
            }

            // 在磁盘上为该分区创建数据目录
            String partitionDir = topicDir + File.separator + id;
            new File(partitionDir).mkdirs();

            Partition partition = new Partition(id, leader, followers, partitionDir);
            partitions.add(partition);

            LOGGER.info("Loaded partition " + id + " for topic " + topic +
                    ", leader: " + leader + ", followers: " + followers);
        }

        topics.put(topic, partitions);
        LOGGER.info("Successfully loaded topic: " + topic + " with " + partitions.size() + " partitions");
    }

    // ======================== 网络连接处理 ========================

    /**
     * 在主循环中不断接受新的客户端 TCP 连接。
     * <p>
     * 该方法运行在一个独立的线程中，循环执行：
     * <ol>
     *   <li>调用 {@code serverChannel.accept()} 尝试接受新连接</li>
     *   <li>如果有新连接到来，将其设为非阻塞模式，提交到线程池中处理</li>
     *   <li>每次循环休眠 100 毫秒，避免空转耗尽 CPU</li>
     * </ol>
     * <p>
     * 当 Broker 关闭（isRunning 变为 false）时循环退出。
     */
    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);
                    LOGGER.info("Accepted connection from " + clientChannel.getRemoteAddress());

                    // 将连接交给线程池处理，主循环继续接受下一个连接
                    executor.submit(() -> handleClient(clientChannel));
                }

                // 短暂休眠，防止空循环占满 CPU
                Thread.sleep(100);
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    /**
     * 处理单个客户端连接——读取请求并分发到对应的处理方法。
     * <p>
     * 由于使用了非阻塞 IO，这里通过循环不断尝试读取数据：
     * <ul>
     *   <li>{@code read() > 0}：成功读取到数据，根据协议处理请求</li>
     *   <li>{@code read() == 0}：没有可用数据（非阻塞模式下正常现象），短暂等待后重试</li>
     *   <li>{@code read() < 0}：客户端关闭了连接，退出循环</li>
     * </ul>
     * <p>
     * 无论正常还是异常退出，finally 块都会确保 Socket 被关闭，释放资源。
     *
     * @param clientChannel 与客户端建立的 Socket 通道
     */
    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);

                if (bytesRead > 0) {
                    buffer.flip(); // 将 Buffer 从"写模式"切换为"读模式"
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    // 返回 -1 表示客户端已关闭连接（TCP FIN）
                    clientChannel.close();
                    break;
                }

                Thread.sleep(50);
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                LOGGER.log(Level.SEVERE, "Error handling client", e);
            }
        } finally {
            // 确保 Socket 被关闭
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }

    /**
     * 根据消息的第一个字节（消息类型标识）将请求分发到对应的处理方法。
     *
     * <h4>支持的消息类型</h4>
     * <table>
     *   <tr><th>类型常量</th><th>含义</th><th>处理方法</th></tr>
     *   <tr><td>PRODUCE (0x00)</td><td>生产者发送消息</td><td>{@link #handleProduceRequest}</td></tr>
     *   <tr><td>FETCH (0x01)</td><td>消费者拉取消息</td><td>{@link #handleFetchRequest}</td></tr>
     *   <tr><td>METADATA (0x02)</td><td>查询集群元数据</td><td>{@link #handleMetadataRequest}</td></tr>
     *   <tr><td>CREATE_TOPIC (0x03)</td><td>创建新主题</td><td>{@link #handleCreateTopicRequest}</td></tr>
     *   <tr><td>REPLICATE (0x04)</td><td>Leader 复制数据到 Follower</td><td>{@link #handleReplicateRequest}</td></tr>
     *   <tr><td>TOPIC_NOTIFICATION (0x05)</td><td>Controller 通知加载新 Topic</td><td>{@link #handleTopicNotification}</td></tr>
     * </table>
     *
     * @param clientChannel 客户端 Socket 通道
     * @param buffer        包含请求数据的 ByteBuffer（已 flip，position 在第一个字节之后）
     * @throws IOException 如果网络读写失败
     */
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();

        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    // ======================== 生产消息（PRODUCE）========================

    /**
     * 处理生产者的"发送消息"请求。
     *
     * <h4>请求格式</h4>
     * <pre>
     * [消息类型 1B] [Topic长度 2B] [Topic名 NB] [分区ID 4B] [消息长度 4B] [消息体 NB]
     * </pre>
     *
     * <h4>处理流程</h4>
     * <ol>
     *   <li>解析 Topic 名、分区 ID、消息内容</li>
     *   <li>验证 Topic 和 分区是否存在</li>
     *   <li>检查当前 Broker 是否为该分区的 Leader：不是则转发给 Leader</li>
     *   <li>将消息追加到分区日志文件末尾，获得偏移量（offset）</li>
     *   <li>如果当前是 Leader，将消息异步复制给所有 Follower</li>
     *   <li>向生产者返回确认响应：消息类型 + 偏移量 + 成功/失败标志</li>
     * </ol>
     *
     * @param clientChannel 客户端 Socket 通道
     * @param buffer        包含请求数据的 ByteBuffer（已跳过消息类型字节）
     * @throws IOException 如果网络读写失败
     */
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // 解析 Topic 名称
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        // 解析分区 ID 和消息内容
        int partition = buffer.getInt();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Produce request for topic: " + topic + ", partition: " + partition);

        // 验证 Topic 是否存在
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        // 查找目标分区
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        // 只有 Leader 才能接受写入——如果当前 Broker 不是 Leader，转发给 Leader
        if (targetPartition.getLeader() != brokerId) {
            forwardProduceToLeader(clientChannel, topic, partition, message, targetPartition.getLeader());
            return;
        }

        // 将消息追加到分区日志
        long offset = targetPartition.append(message);

        // 作为 Leader，将消息复制给所有 Follower 以保证数据一致性
        replicateToFollowers(topic, targetPartition, message, offset);

        // 构造并发送成功响应
        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);       // 消息在分区中的偏移量
        response.put((byte) (offset > -1 ? 0 : 1)); // 0=成功, 1=失败
        response.flip();
        clientChannel.write(response);
    }

    /**
     * 将生产请求转发给分区的 Leader Broker。
     * <p>
     * 当生产者连接到 Follower 并尝试写入时，Follower 不会直接拒绝，
     * 而是自动将请求转发给该分区的 Leader，让 Leader 处理写入。
     * 转发完成后，将 Leader 的响应原样返回给生产者。
     *
     * @param clientChannel 与生产者之间的 Socket 通道
     * @param topic         目标主题
     * @param partition     目标分区
     * @param message       消息内容
     * @param leaderId      Leader Broker 的 ID
     * @throws IOException 如果与 Leader 的网络通信失败
     */
    private void forwardProduceToLeader(SocketChannel clientChannel, String topic, int partition,
                                        byte[] message, int leaderId) throws IOException {
        BrokerInfo leader = clusterMetadata.get(leaderId);
        if (leader == null) {
            Protocol.sendErrorResponse(clientChannel, "Leader broker not available");
            return;
        }

        // 建立到 Leader 的短连接
        try (SocketChannel leaderChannel = SocketChannel.open()) {
            leaderChannel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            // 构造转发请求（格式与原请求相同）
            ByteBuffer request = ByteBuffer.allocate(9 + topic.length() + message.length);
            request.put(Protocol.PRODUCE);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(partition);
            request.putInt(message.length);
            request.put(message);
            request.flip();

            leaderChannel.write(request);

            // 读取 Leader 的响应，转发回生产者
            ByteBuffer response = ByteBuffer.allocate(10);
            leaderChannel.read(response);
            response.flip();
            clientChannel.write(response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward produce request to leader", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
        }
    }

    /**
     * 将消息异步复制到所有 Follower Broker。
     * <p>
     * <b>为什么需要复制？</b>保证数据的高可用性——即使 Leader 所在 Broker 崩溃，
     * 仍然可以从 Follower 中恢复数据，不会丢失消息。
     * <p>
     * 复制过程：
     * <ol>
     *   <li>遍历该分区的所有 Follower ID，跳过自身</li>
     *   <li>对每个 Follower，在线程池中异步发送复制请求（避免阻塞生产者响应）</li>
     *   <li>复制请求包含：Topic、分区、偏移量、消息内容</li>
     *   <li>Follower 处理完成后返回确认（ACK）</li>
     * </ol>
     *
     * @param topic     主题名称
     * @param partition 分区对象
     * @param message   要复制的消息内容
     * @param offset    消息在分区中的偏移量
     */
    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        for (int followerId : partition.getFollowers()) {
            if (followerId == brokerId)
                continue; // 跳过自身

            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null)
                continue;

            // 异步复制：不阻塞主线程，让生产者的响应尽快返回
            executor.submit(() -> {
                try (SocketChannel followerChannel = SocketChannel.open()) {
                    followerChannel.connect(new InetSocketAddress(follower.getHost(), follower.getPort()));

                    // 构造复制请求
                    ByteBuffer request = ByteBuffer.allocate(17 + topic.length() + message.length);
                    request.put(Protocol.REPLICATE);
                    request.putShort((short) topic.length());
                    request.put(topic.getBytes());
                    request.putInt(partition.getId());
                    request.putLong(offset);
                    request.putInt(message.length);
                    request.put(message);
                    request.flip();

                    followerChannel.write(request);

                    // 等待确认
                    ByteBuffer response = ByteBuffer.allocate(1);
                    followerChannel.read(response);
                    response.flip();

                    byte ack = response.get();
                    LOGGER.info("Replication to follower " + followerId + " " +
                            (ack == Protocol.REPLICATE_ACK ? "succeeded" : "failed"));
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Replication to follower " + followerId + " failed", e);
                }
            });
        }
    }

    /**
     * 处理来自 Leader 的复制请求（作为 Follower 接收消息）。
     *
     * <h4>请求格式</h4>
     * <pre>
     * [消息类型 1B] [Topic长度 2B] [Topic名 NB] [分区ID 4B] [偏移量 8B] [消息长度 4B] [消息体 NB]
     * </pre>
     * <p>
     * Follower 收到复制请求后，直接将消息追加到本地对应的分区日志中，
     * 然后返回确认（ACK）。注意：Follower 不会再次复制——复制只有一轮。
     *
     * @param clientChannel 与 Leader 之间的 Socket 通道
     * @param buffer        包含请求数据的 ByteBuffer（已跳过消息类型字节）
     * @throws IOException 如果网络读写失败
     */
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // 解析请求
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Replication request for topic: " + topic + ", partition: " + partitionId + ", offset: " + offset);

        // 验证 Topic 是否存在
        if (!topics.containsKey(topic)) {
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // 复制失败
            response.flip();
            clientChannel.write(response);
            return;
        }

        // 查找目标分区
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partitionId) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // 复制失败
            response.flip();
            clientChannel.write(response);
            return;
        }

        // 将消息写入本地分区（作为 Follower 的副本）
        long appendedOffset = targetPartition.append(message);

        // 返回确认
        ByteBuffer response = ByteBuffer.allocate(1);
        response.put(Protocol.REPLICATE_ACK); // 复制成功
        response.flip();
        clientChannel.write(response);
    }

    // ======================== 消费消息（FETCH）========================

    /**
     * 处理消费者的"拉取消息"请求。
     *
     * <h4>请求格式</h4>
     * <pre>
     * [消息类型 1B] [Topic长度 2B] [Topic名 NB] [分区ID 4B] [起始偏移量 8B] [最大字节数 4B]
     * </pre>
     *
     * <h4>处理流程</h4>
     * <ol>
     *   <li>解析 Topic、分区、起始偏移量（offset）、最大读取字节数</li>
     *   <li>验证 Topic 和 分区是否存在</li>
     *   <li>检查 offset 是否有效：如果 offset >= 分区当前末尾偏移量，说明没有新消息</li>
     *   <li>从分区日志中读取消息（从指定 offset 开始，不超过 maxBytes）</li>
     *   <li>返回响应：消息数量 + 每条消息的「偏移量 + 长度 + 内容」</li>
     * </ol>
     *
     * <h4>响应格式</h4>
     * <pre>
     * [消息类型 1B] [消息数量 4B] [偏移量1 8B] [长度1 4B] [消息体1 NB] [偏移量2 ...]
     * </pre>
     *
     * @param clientChannel 客户端 Socket 通道
     * @param buffer        包含请求数据的 ByteBuffer（已跳过消息类型字节）
     * @throws IOException 如果网络读写失败
     */
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // 解析请求参数
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        LOGGER.info("Fetch request for topic: " + topic + ", partition: " + partition +
                ", offset: " + offset + ", maxBytes: " + maxBytes);

        // 验证 Topic 是否存在
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        // 查找目标分区
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        // 检查 offset 是否有效——如果消费者请求的 offset 已经超出当前日志末尾，
        // 说明没有新消息可消费，返回空消息列表
        if (offset >= targetPartition.getLogEndOffset()) {
            ByteBuffer response = ByteBuffer.allocate(5);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0); // 0 条消息
            response.flip();
            clientChannel.write(response);
            return;
        }

        // 从分区日志中读取消息
        List<byte[]> messages = targetPartition.readMessages(offset, maxBytes);

        // 计算响应总大小
        int totalSize = 5; // 1B 响应类型 + 4B 消息数量
        for (byte[] msg : messages) {
            totalSize += 12 + msg.length; // 每条消息：8B offset + 4B 长度 + NB 消息体
        }

        // 构造响应
        ByteBuffer response = ByteBuffer.allocate(totalSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset);
            response.putInt(msg.length);
            response.put(msg);
            currentOffset++; // 每条消息的偏移量递增 1
        }

        response.flip();
        clientChannel.write(response);
    }

    // ======================== 元数据查询（METADATA）========================

    /**
     * 处理客户端的"查询集群元数据"请求。
     * <p>
     * 元数据查询是 Kafka 协议中的关键步骤：生产者和消费者在发送/拉取消息之前，
     * 必须先查询元数据来获取集群拓扑信息。
     *
     * <h4>返回的信息</h4>
     * <ul>
     *   <li><b>Broker 列表</b>：集群中所有 Broker 的 ID、主机名、端口</li>
     *   <li><b>Topic 列表</b>：每个 Topic 包含哪些分区，每个分区的 Leader 和 Follower 分别是谁</li>
     * </ul>
     *
     * <h4>为什么客户端需要元数据？</h4>
     * 知道了每个分区的 Leader 在哪个 Broker 后，生产者才能把消息发给正确的 Broker；
     * 消费者才能知道从哪个 Broker 拉取消息。
     *
     * @param clientChannel 客户端 Socket 通道
     * @param buffer        请求数据（METADATA 请求没有额外参数）
     * @throws IOException 如果网络读写失败
     */
    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // ---- 第一步：计算响应所需的总字节数 ----

        int size = 5; // 1B 响应类型 + 4B Topic 数量

        // 计算 Topic 元数据占用的字节数
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            size += 6 + entry.getKey().length(); // 2B 名称长度 + 名称 + 4B 分区数量

            // 每个分区：4B ID + 4B Leader + 4B Follower 数量
            size += entry.getValue().size() * 12;

            // 每个 Follower 的 ID 占 4B
            for (Partition partition : entry.getValue()) {
                size += partition.getFollowers().size() * 4;
            }
        }

        // 计算 Broker 元数据占用的字节数
        size += 4; // 4B Broker 数量
        size += clusterMetadata.size() * 10; // 每个 Broker：4B ID + 2B 主机名长度 + 4B 端口

        // 加上每台 Broker 的主机名字符串长度
        for (BrokerInfo broker : clusterMetadata.values()) {
            size += broker.getHost().length();
        }

        // ---- 第二步：构造响应数据 ----

        ByteBuffer response = ByteBuffer.allocate(size);
        response.put(Protocol.METADATA_RESPONSE);

        // 写入 Broker 信息
        response.putInt(clusterMetadata.size());
        for (BrokerInfo broker : clusterMetadata.values()) {
            response.putInt(broker.getId());
            response.putShort((short) broker.getHost().length());
            response.put(broker.getHost().getBytes());
            response.putInt(broker.getPort());
        }

        // 写入 Topic 信息（以及每个 Topic 下的分区信息）
        response.putInt(topics.size());
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            response.putShort((short) topic.length());
            response.put(topic.getBytes());
            response.putInt(partitions.size());

            for (Partition partition : partitions) {
                response.putInt(partition.getId());
                response.putInt(partition.getLeader());

                List<Integer> followers = partition.getFollowers();
                response.putInt(followers.size());
                for (Integer follower : followers) {
                    response.putInt(follower);
                }
            }
        }

        response.flip();
        clientChannel.write(response);
    }

    // ======================== 创建 Topic（CREATE_TOPIC）========================

    /**
     * 处理客户端的"创建主题"请求。
     *
     * <h4>请求格式</h4>
     * <pre>
     * [消息类型 1B] [Topic长度 2B] [Topic名 NB] [分区数 4B] [副本因子 2B]
     * </pre>
     *
     * <h4>处理流程</h4>
     * <ol>
     *   <li>验证 Topic 是否已存在、参数是否合法</li>
     *   <li>如果当前 Broker 是 Controller → 直接创建</li>
     *   <li>如果不是 Controller → 将请求转发给 Controller 处理</li>
     * </ol>
     * <p>
     * <b>为什么只有 Controller 能创建 Topic？</b>只有 Controller 拥有全局视角，
     * 知道集群中有哪些 Broker，才能合理地将分区分配给不同的 Broker。
     *
     * @param clientChannel 客户端 Socket 通道
     * @param buffer        包含请求数据的 ByteBuffer（已跳过消息类型字节）
     * @throws IOException 如果网络读写失败
     */
    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        LOGGER.info("Create topic request: " + topic +
                ", partitions: " + numPartitions +
                ", replication: " + replicationFactor);

        // 验证：Topic 不能重复创建
        if (topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic already exists");
            return;
        }

        // 验证：分区数和副本因子必须合法
        // 副本因子不能超过集群中的 Broker 数量（每个副本必须在不同的 Broker 上）
        if (numPartitions <= 0 || replicationFactor <= 0 ||
                replicationFactor > clusterMetadata.size()) {
            Protocol.sendErrorResponse(clientChannel, "Invalid topic configuration");
            return;
        }

        if (isController.get()) {
            // 当前 Broker 就是 Controller，直接创建
            createTopic(topic, numPartitions, replicationFactor);

            // 返回成功响应
            ByteBuffer response = ByteBuffer.allocate(2);
            response.put(Protocol.CREATE_TOPIC_RESPONSE);
            response.put((byte) 0); // 0 = 成功
            response.flip();
            clientChannel.write(response);
        } else {
            // 当前 Broker 不是 Controller，转发给 Controller
            forwardCreateTopicToController(clientChannel, topic, numPartitions, replicationFactor);
        }
    }

    /**
     * 将"创建 Topic"请求转发给 Controller Broker。
     * <p>
     * 转发逻辑与 {@link #forwardProduceToLeader} 类似，区别是这里是从 ZooKeeper 查找 Controller 地址。
     *
     * @param clientChannel     与客户端之间的 Socket 通道
     * @param topic             要创建的主题名
     * @param numPartitions     分区数量
     * @param replicationFactor 副本因子（每个分区有几个副本）
     * @throws IOException 如果与 Controller 的网络通信失败
     */
    private void forwardCreateTopicToController(SocketChannel clientChannel, String topic,
                                                int numPartitions, short replicationFactor) throws IOException {
        // 从 ZooKeeper 获取当前 Controller 的 ID
        int controllerId = -1;
        try {
            String controllerData = zkClient.getData("/controller");
            controllerId = Integer.parseInt(controllerData);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get controller info", e);
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        BrokerInfo controller = clusterMetadata.get(controllerId);
        if (controller == null) {
            Protocol.sendErrorResponse(clientChannel, "Controller broker not available");
            return;
        }

        try (SocketChannel controllerChannel = SocketChannel.open()) {
            controllerChannel.connect(new InetSocketAddress(controller.getHost(), controller.getPort()));

            // 构造转发请求
            ByteBuffer request = ByteBuffer.allocate(9 + topic.length());
            request.put(Protocol.CREATE_TOPIC);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(numPartitions);
            request.putShort(replicationFactor);
            request.flip();

            controllerChannel.write(request);

            // 读取 Controller 的响应，转发回客户端
            ByteBuffer response = ByteBuffer.allocate(2);
            controllerChannel.read(response);
            response.flip();
            clientChannel.write(response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller");
        }
    }

    /**
     * Controller 执行 Topic 创建的核心逻辑。
     * <p>
     * <b>只有 Controller 能调用。</b>如果当前 Broker 不是 Controller，直接返回。
     *
     * <h4>创建流程</h4>
     * <ol>
     *   <li>在本地磁盘创建 Topic 数据目录</li>
     *   <li>在 ZooKeeper 创建 Topic 节点和 partitions 子节点</li>
     *   <li>为每个分区分配 Leader 和 Follower（轮询策略，保证均匀分布）</li>
     *   <li>将每个分区的元数据写入 ZooKeeper</li>
     *   <li>将 Topic 加入内存 Map</li>
     *   <li>通知集群中所有其他 Broker 加载这个新 Topic</li>
     * </ol>
     *
     * <h4>分区分片策略（简化版）</h4>
     * 使用轮询（Round-Robin）方式分配 Leader：
     * <ul>
     *   <li>分区 0 的 Leader = Broker 0，分区 1 的 Leader = Broker 1，以此类推</li>
     *   <li>Follower 依次选择 Leader 之后的 Broker</li>
     * </ul>
     * 这确保了 Leader 角色尽量均匀地分布在所有 Broker 上，避免单个 Broker 负载过高。
     *
     * @param topic             要创建的主题名称
     * @param numPartitions     分区数量（决定并发度）
     * @param replicationFactor 副本因子（决定数据冗余度，1=无冗余，3=容忍2个节点故障）
     */
    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            LOGGER.warning("Only the controller can create topics");
            return;
        }

        try {
            // 创建本地数据目录
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();

            // 在 ZooKeeper 中创建 Topic 的层级结构
            String topicPath = "/topics/" + topic;
            if (!zkClient.exists(topicPath)) {
                zkClient.createPersistentNode(topicPath, "");
                zkClient.createPersistentNode(topicPath + "/partitions", "");
            }

            // 创建所有分区
            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());

            for (int i = 0; i < numPartitions; i++) {
                int partitionId = i;
                String partitionDir = topicDir + File.separator + partitionId;
                new File(partitionDir).mkdirs();

                // 轮询选择 Leader：分区 i 的 Leader = 第 (i % Broker数量) 个 Broker
                int leaderIndex = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex);

                // 选择 Follower：依次选择 Leader 之后的 Broker
                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor; j++) {
                    int followerIndex = (leaderIndex + j) % brokerIds.size();
                    followers.add(brokerIds.get(followerIndex));
                }

                // 创建 Partition 对象
                Partition partition = new Partition(partitionId, leaderId, followers, partitionDir);
                partitions.add(partition);

                // 将分区元数据写入 ZooKeeper（持久化节点，Broker 重启后仍存在）
                String partitionPath = topicPath + "/partitions/" + partitionId;
                String partitionData = leaderId + ";";
                for (int follower : followers) {
                    partitionData += follower + ",";
                }

                zkClient.createPersistentNode(partitionPath, partitionData);

                LOGGER.info("Created partition " + partitionId +
                        " for topic " + topic +
                        " with leader " + leaderId +
                        " and followers " + followers);
            }

            // Controller 自身加载新 Topic
            topics.put(topic, partitions);

            // 通知集群中所有其他 Broker 加载这个新 Topic
            for (int brokerId : brokerIds) {
                if (brokerId != this.brokerId) {
                    notifyBrokerForTopicCreation(brokerId, topic);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
        }
    }

    // ======================== Topic 通知 ========================

    /**
     * Controller 通知某个 Broker："有新 Topic 被创建了，请从 ZooKeeper 加载它"。
     * <p>
     * 通过网络直接发送通知（而非依赖 ZooKeeper 的 Watch 机制），因为：
     * <ul>
     *   <li>更快速：直接 TCP 连接，延迟低</li>
     *   <li>更可靠：可能 ZooKeeper 的 Watch 触发有延迟</li>
     * </ul>
     *
     * <h4>通知格式</h4>
     * <pre>
     * [消息类型=TOPIC_NOTIFICATION 1B] [Topic名称长度 2B] [Topic名称 NB]
     * </pre>
     *
     * @param brokerId 要通知的目标 Broker ID
     * @param topic    新创建的 Topic 名称
     */
    private void notifyBrokerForTopicCreation(int brokerId, String topic) {
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if (broker == null)
            return;

        // 异步发送通知，不阻塞主流程
        executor.submit(() -> {
            try (SocketChannel brokerChannel = SocketChannel.open()) {
                brokerChannel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));

                // 构造通知消息
                ByteBuffer request = ByteBuffer.allocate(3 + topic.length());
                request.put(Protocol.TOPIC_NOTIFICATION);
                request.putShort((short) topic.length());
                request.put(topic.getBytes());
                request.flip();

                brokerChannel.write(request);

                // 等待确认
                ByteBuffer response = ByteBuffer.allocate(1);
                brokerChannel.read(response);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to notify broker " + brokerId + " about topic creation", e);
            }
        });
    }

    /**
     * 接收来自 Controller 的 Topic 创建通知。
     * <p>
     * Controller 创建完新 Topic 后，向所有其他 Broker 发送通知，
     * 收到通知的 Broker 从 ZooKeeper 读取并加载该 Topic 的完整信息。
     * <p>
     * 加载成功后返回确认（0），失败返回错误码（1）。
     *
     * @param clientChannel 与 Controller 之间的 Socket 通道
     * @param buffer        包含通知数据的 ByteBuffer（已跳过消息类型字节）
     * @throws IOException 如果网络读写失败
     */
    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        LOGGER.info("Received topic notification for: " + topic);

        // 从 ZooKeeper 加载 Topic 信息
        try {
            loadTopic(topic);

            // 返回成功确认
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // 0 = 成功
            response.flip();
            clientChannel.write(response);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);

            // 返回错误码
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 1); // 1 = 失败
            response.flip();
            clientChannel.write(response);
        }
    }

    // ======================== 程序入口 ========================

    /**
     * SimpleKafka Broker 的启动入口。
     *
     * <h4>命令行参数</h4>
     * <pre>
     * java SimpleKafkaBroker <brokerId> <host> <port> [zkPort]
     *
     *   brokerId : Broker 的唯一数字编号（必填），例如 0
     *   host     : 绑定的主机名或 IP（必填），例如 127.0.0.1
     *   port     : 监听的 TCP 端口号（必填），例如 9092
     *   zkPort   : ZooKeeper 端口号（可选），默认 2181
     * </pre>
     *
     * <h4>使用示例</h4>
     * <pre>
     *   # 启动 Broker 0，监听 9092 端口
     *   java SimpleKafkaBroker 0 127.0.0.1 9092
     *
     *   # 启动 Broker 1，监听 9093 端口，连接 2181 端口的 ZooKeeper
     *   java SimpleKafkaBroker 1 127.0.0.1 9093 2181
     * </pre>
     *
     * <h4>优雅关闭</h4>
     * 程序注册了 JVM 关闭钩子（Shutdown Hook），按下 Ctrl+C 或 kill 进程时，
     * 会自动调用 {@link #stop()} 方法优雅关闭 Broker。
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaBroker <brokerId> <host> <port> [zkPort]");
            System.exit(1);
        }

        try {
            int brokerId = Integer.parseInt(args[0]);
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            int zkPort = args.length > 3 ? Integer.parseInt(args[3]) : 2181;

            SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
            broker.start();

            // 注册关闭钩子——JVM 退出时自动调用 stop() 释放资源
            Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

            System.out.println("SimpleKafka broker started. Press Ctrl+C to stop.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start broker", e);
        }
    }

    // ======================== Getter 方法 ========================

    /** @return 当前 Broker 的唯一编号 */
    public int getBrokerId() {
        return brokerId;
    }

    /** @return 当前 Broker 绑定的主机地址 */
    public String getBrokerHost() {
        return brokerHost;
    }

    /** @return 当前 Broker 监听的端口号 */
    public int getBrokerPort() {
        return brokerPort;
    }

    /** @return 当前 Broker 管理的所有 Topic 及其分区（线程安全的 Map） */
    public Map<String, List<Partition>> getTopics() {
        return topics;
    }

    /** @return 线程池实例 */
    public ExecutorService getExecutor() {
        return executor;
    }

    /** @return 服务端 Socket 通道 */
    public ServerSocketChannel getServerChannel() {
        return serverChannel;
    }

    /** @return 运行状态标识（true=运行中，false=已停止） */
    public AtomicBoolean getIsRunning() {
        return isRunning;
    }

    /** @return 控制器状态标识（true=当前 Broker 是 Controller） */
    public AtomicBoolean getIsController() {
        return isController;
    }

    /** @return 集群中所有 Broker 的信息缓存 */
    public Map<Integer, BrokerInfo> getClusterMetadata() {
        return clusterMetadata;
    }

    /** @return ZooKeeper 客户端实例 */
    public ZookeeperClient getZkClient() {
        return zkClient;
    }
}
