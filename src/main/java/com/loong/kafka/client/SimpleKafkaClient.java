package com.loong.kafka.client;


import com.loong.kafka.broker.Protocol;
import com.loong.kafka.broker.Protocol.BrokerInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * SimpleKafka 核心客户端 —— 与 Kafka Broker 进行底层通信的客户端。
 *
 * <h2>这个类是做什么的？</h2>
 * <p>它是整个 SimpleKafka 项目中最核心的类，负责通过原始 TCP Socket 与 Kafka Broker
 * 直接通信。它不依赖任何第三方 Kafka 库，而是自己实现了 Kafka 的二进制协议。</p>
 *
 * <h2>Kafka 基础概念速览</h2>
 * <ul>
 *   <li><b>Broker（代理节点）</b>：Kafka 集群中的一台服务器，负责存储和转发消息。</li>
 *   <li><b>Topic（主题）</b>：消息的分类，类似数据库中的"表"。生产者往 Topic 发消息，
 *       消费者从 Topic 读消息。</li>
 *   <li><b>Partition（分区）</b>：每个 Topic 可以被分成多个分区，分布在不同 Broker 上，
 *       实现并行处理和水平扩展。每个分区是一个有序的、不可变的消息队列。</li>
 *   <li><b>Offset（偏移量）</b>：每条消息在分区中的唯一递增编号，从 0 开始。消费者用
 *       它来记录"读到哪里了"。</li>
 *   <li><b>Leader / Follower</b>：每个分区有一个 Leader 副本（负责读写）和多个
 *       Follower 副本（只负责备份）。</li>
 *   <li><b>元数据（Metadata）</b>：描述集群结构的信息，包括有哪些 Broker、有哪些 Topic、
 *       每个 Topic 有多少分区、每个分区的 Leader 是谁等。</li>
 * </ul>
 *
 * <h2>工作流程</h2>
 * <ol>
 *   <li>初始化时连接 Bootstrap Broker，拉取集群元数据（refreshMetadata）</li>
 *   <li>发送消息时，根据元数据找到目标分区所在的 Leader Broker，直接连接发送</li>
 *   <li>消费消息时，同样找到 Leader Broker，发送 Fetch 请求拉取消息</li>
 * </ol>
 *
 * @see SimpleKafkaProducer 生产者（封装了本类，提供更友好的发送接口）
 * @see SimpleKafkaConsumer 消费者（封装了本类，提供更友好的消费接口）
 */
public class SimpleKafkaClient {

    /** 日志记录器，用于输出运行时的调试和错误信息 */
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());

    /** Socket 读取缓冲区的默认大小（4KB），用于接收 Broker 的响应数据 */
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    /**
     * Bootstrap Broker 的主机名或 IP 地址。
     * "Bootstrap" 意为"引导"——我们只需要知道集群中任意一个 Broker 的地址，
     * 就能通过它获取整个集群的元数据，从而发现所有其他 Broker。
     */
    private final String bootstrapBroker;

    /** Bootstrap Broker 的端口号（Kafka 默认使用 9092） */
    private final int bootstrapPort;

    /**
     * Topic 元数据缓存，Key 是 Topic 名称，Value 是该 Topic 的元数据（分区列表等）。
     * 使用 ConcurrentHashMap 保证线程安全，因为生产者和消费者可能在不同线程中
     * 同时访问这些元数据。
     */
    private final Map<String, TopicMetadata> topicMetadata;

    /**
     * Broker 信息缓存，Key 是 Broker ID，Value 是 Broker 的连接信息（主机、端口）。
     * 从元数据响应中获取并缓存，这样发送消息时就知道该连接哪个 Broker。
     */
    private final Map<Integer, BrokerInfo> brokers;

    /**
     * 关联 ID（Correlation ID）生成器。
     * 每次向 Broker 发送请求时都会附带一个唯一的 correlation ID，
     * Broker 的响应中会带回这个 ID，这样客户端就能把响应和请求对应起来。
     * 使用 AtomicInteger 保证线程安全的自增操作。
     */
    private final AtomicInteger correlationId;

    /**
     * 构造一个 SimpleKafka 客户端实例。
     *
     * @param bootstrapBroker 集群中任意一个 Broker 的主机名或 IP
     * @param bootstrapPort   该 Broker 的端口号
     */
    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
        this.brokers = new ConcurrentHashMap<>();
        this.correlationId = new AtomicInteger(0);
    }

    /**
     * 初始化客户端 —— 连接 Bootstrap Broker 并拉取集群元数据。
     * <p>
     * 必须在发送或接收消息之前调用此方法。它会：
     * <ol>
     *   <li>连接到配置的 Bootstrap Broker</li>
     *   <li>发送元数据请求，获取集群中所有 Broker 和 Topic 的信息</li>
     *   <li>将获取到的信息缓存到内存中，供后续操作使用</li>
     * </ol>
     *
     * @throws IOException 如果网络连接失败或 Broker 返回错误
     */
    public void initialize() throws IOException {
        refreshMetadata();
    }

    /**
     * 刷新集群元数据 —— 重新从 Broker 拉取最新的集群信息。
     *
     * <h3>什么时候需要刷新？</h3>
     * <ul>
     *   <li>客户端初始化时（第一次获取集群信息）</li>
     *   <li>创建新 Topic 后（需要知道新 Topic 的分区信息）</li>
     *   <li>发送/消费消息时发现本地缓存中没有对应 Topic 的信息</li>
     *   <li>Leader 发生变更时（实际 Kafka 中很常见，本项目中较少）</li>
     * </ul>
     *
     * <h3>通信流程</h3>
     * <ol>
     *   <li>通过 TCP Socket 连接到 Bootstrap Broker</li>
     *   <li>发送一个"元数据请求"（由 Protocol 类编码为二进制格式）</li>
     *   <li>读取 Broker 返回的二进制响应</li>
     *   <li>将响应解码为 Broker 列表和 Topic 列表</li>
     *   <li>更新本地缓存</li>
     * </ol>
     *
     * @throws IOException 如果网络连接失败、未收到数据、或 Broker 返回错误
     */
    public void refreshMetadata() throws IOException {
        // 使用 try-with-resources 确保 SocketChannel 在使用完毕后自动关闭
        try (SocketChannel channel = SocketChannel.open()) {
            // 建立到 Bootstrap Broker 的 TCP 连接
            channel.connect(new InetSocketAddress(bootstrapBroker, bootstrapPort));

            // 第一步：发送元数据请求
            // Protocol.encodeMetadataRequest() 将请求编码为二进制 ByteBuffer
            ByteBuffer request = Protocol.encodeMetadataRequest();
            channel.write(request);

            // 第二步：读取 Broker 的响应
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            // flip() 将 Buffer 从"写模式"切换到"读模式"
            // 在写模式下 position 指向已写入数据的末尾，flip() 后将 position 归零，
            // limit 设为原 position，这样后续的 get() 操作就能从开头读取数据
            response.flip();

            // 第三步：解码响应，得到结构化的元数据
            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Failed to fetch metadata: " + result.getError());
            }

            // 第四步：更新 Broker 信息缓存
            brokers.clear();
            for (BrokerInfo broker : result.getBrokers()) {
                brokers.put(broker.getId(), new BrokerInfo(broker.getId(), broker.getHost(), broker.getPort()));
            }

            // 第五步：更新 Topic 元数据缓存
            // 遍历每个 Topic，将其分区信息转换为内部数据结构
            topicMetadata.clear();
            for (Protocol.TopicMetadata topic : result.getTopics()) {
                List<PartitionInfo> partitions = new ArrayList<>();

                for (Protocol.PartitionMetadata partition : topic.getPartitions()) {
                    partitions.add(new PartitionInfo(
                            partition.getId(),       // 分区 ID（从 0 开始编号）
                            partition.getLeader(),   // Leader Broker 的 ID
                            partition.getReplicas()  // 所有副本所在的 Broker ID 列表
                    ));
                }

                topicMetadata.put(topic.getName(), new TopicMetadata(topic.getName(), partitions));
            }

            LOGGER.info("Metadata refreshed: " + brokers.size() + " brokers, " +
                    topicMetadata.size() + " topics");
        }
    }

    /**
     * 创建一个新的 Topic。
     *
     * <h3>Kafka 中创建 Topic 的含义</h3>
     * <p>创建 Topic 就是告诉 Kafka 集群："我需要一个新的消息分类，它有 N 个分区，
     * 每个分区保存 R 个副本"。Broker 会分配资源并在所有相关节点上创建对应的
     * 日志文件。</p>
     *
     * <h3>本方法的流程</h3>
     * <ol>
     *   <li>从缓存中任选一个 Broker（如果缓存为空则先刷新元数据）</li>
     *   <li>向该 Broker 发送"创建 Topic"请求</li>
     *   <li>等待 Broker 响应，检查是否创建成功</li>
     *   <li>如果创建成功，刷新本地元数据以包含新的 Topic 信息</li>
     * </ol>
     *
     * @param topic             要创建的 Topic 名称
     * @param numPartitions     分区数量（决定了该 Topic 的并行处理能力）
     * @param replicationFactor 副本因子（每个分区的副本数，用于容错）
     * @return true 表示创建成功，false 表示创建失败
     * @throws IOException 如果网络通信出现异常
     */
    public boolean createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        // 确保至少知道一个 Broker 的地址
        if (brokers.isEmpty()) {
            refreshMetadata();
            if (brokers.isEmpty()) {
                throw new IOException("No brokers available");
            }
        }

        // 取任意一个可用的 Broker 来发送创建请求
        BrokerInfo broker = brokers.values().iterator().next();

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));

            // 发送创建 Topic 的请求
            ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);
            channel.write(request);

            // 读取响应
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            // 响应的第一个字节是响应类型（Response Type），用于区分不同的响应
            byte responseType = response.get();
            if (responseType != Protocol.CREATE_TOPIC_RESPONSE) {
                // 如果返回的是错误响应，提取错误信息
                if (responseType == Protocol.ERROR_RESPONSE) {
                    short errorLength = response.getShort();
                    byte[] errorBytes = new byte[errorLength];
                    response.get(errorBytes);
                    String error = new String(errorBytes);
                    LOGGER.warning("Error creating topic: " + error);
                    return false;
                }
                throw new IOException("Invalid create topic response type: " + responseType);
            }

            // 读取状态字节：0 表示成功，非 0 表示失败
            byte status = response.get();
            boolean success = status == 0;

            if (success) {
                // 创建成功后刷新元数据，这样后续操作就能找到新 Topic 了
                refreshMetadata();
            }

            return success;
        }
    }

    /**
     * 向指定 Topic 的指定分区发送一条消息。
     *
     * <h3>消息如何到达正确的 Broker？</h3>
     * <ol>
     *   <li>先查看本地元数据缓存，找到目标 Topic 的分区信息</li>
     *   <li>确定目标分区当前所在的 Leader Broker</li>
     *   <li>直接连接该 Leader Broker 并发送消息</li>
     *   <li>Leader Broker 会将消息写入本地日志，Follower 副本随后同步</li>
     * </ol>
     *
     * @param topic     目标 Topic 名称
     * @param partition 目标分区 ID（从 0 开始）
     * @param message   要发送的消息内容（字节数组，可以是任意二进制数据）
     * @return Broker 返回的 Offset（消息在分区中的位置编号），可用于后续消费
     * @throws IOException 如果 Topic/分区不存在、Leader 不可达、或网络失败
     */
    public long send(String topic, int partition, byte[] message) throws IOException {
        // 第一步：确保本地有该 Topic 的元数据
        if (!topicMetadata.containsKey(topic)) {
            refreshMetadata();
            if (!topicMetadata.containsKey(topic)) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        // 第二步：找到目标分区的元数据（主要是找到 Leader）
        TopicMetadata metadata = topicMetadata.get(topic);
        PartitionInfo partitionInfo = null;

        for (PartitionInfo info : metadata.getPartitions()) {
            if (info.getId() == partition) {
                partitionInfo = info;
                break;
            }
        }

        if (partitionInfo == null) {
            throw new IOException("Partition not found: " + partition);
        }

        // 第三步：根据分区元数据中的 Leader ID，找到 Leader Broker 的连接信息
        int leaderId = partitionInfo.getLeader();
        BrokerInfo leader = brokers.get(leaderId);

        if (leader == null) {
            // Leader 信息不在缓存中，刷新元数据再试一次
            refreshMetadata();
            leader = brokers.get(leaderId);

            if (leader == null) {
                throw new IOException("Leader broker not found: " + leaderId);
            }
        }

        // 第四步：连接到 Leader Broker 并发送消息
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            // 发送"生产消息"请求
            ByteBuffer request = Protocol.encodeProducerRequest(topic, partition, message);
            channel.write(request);

            // 读取响应
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            // 解码响应，获取 Broker 分配的消息 Offset
            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Failed to produce message: " + result.getError());
            }

            // 返回 Broker 分配的消息 Offset
            // 这个 Offset 很重要！消费者需要用它来定位消息的位置
            return result.getOffset();
        }
    }

    /**
     * 从指定 Topic 的指定分区拉取消息（从某个 Offset 开始）。
     *
     * <h3>消费消息的核心概念</h3>
     * <p>消费者通过 Offset 来记录"读到哪里了"。比如上次读到了 Offset=5，
     * 那下次就从 Offset=5 开始读取新消息。这保证了消息不会被重复处理，
     * 也不会漏掉消息（前提是 Offset 被正确管理）。</p>
     *
     * <h3>参数说明</h3>
     * <ul>
     *   <li><b>offset</b>：从哪个位置开始读。0 表示从分区的第一条消息开始</li>
     *   <li><b>maxBytes</b>：本次拉取最多返回多少字节的数据，防止一次返回太多消息</li>
     * </ul>
     *
     * @param topic     目标 Topic 名称
     * @param partition 目标分区 ID
     * @param offset    起始 Offset（从这个位置开始读取消息）
     * @param maxBytes  本次拉取的最大字节数
     * @return 消息列表，每个元素是一条消息的字节数组。如果没有新消息则返回空列表
     * @throws IOException 如果 Topic/分区不存在、Leader 不可达、或网络失败
     */
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        // 第一步：确保有该 Topic 的元数据
        if (!topicMetadata.containsKey(topic)) {
            refreshMetadata();
            if (!topicMetadata.containsKey(topic)) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        // 第二步：找到目标分区的 Leader Broker
        TopicMetadata metadata = topicMetadata.get(topic);
        PartitionInfo partitionInfo = null;

        for (PartitionInfo info : metadata.getPartitions()) {
            if (info.getId() == partition) {
                partitionInfo = info;
                break;
            }
        }

        if (partitionInfo == null) {
            throw new IOException("Partition not found: " + partition);
        }

        int leaderId = partitionInfo.getLeader();
        BrokerInfo leader = brokers.get(leaderId);

        if (leader == null) {
            refreshMetadata();
            leader = brokers.get(leaderId);

            if (leader == null) {
                throw new IOException("Leader broker not found: " + leaderId);
            }
        }

        // 第三步：连接 Leader Broker 并发送"拉取消息"请求
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            ByteBuffer request = Protocol.encodeFetchRequest(topic, partition, offset, maxBytes);
            channel.write(request);

            // 读取响应
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            Protocol.FetchResult result = Protocol.decodeFetchResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Failed to fetch messages: " + result.getError());
            }

            // 将字节数组收集到列表中返回
            List<byte[]> messages = new ArrayList<>();
            for (byte[] msg : result.getMessages()) {
                messages.add(msg);
            }

            return messages;
        }
    }

    /**
     * 获取所有已知 Topic 的元数据。
     * <p>返回的是一个副本（HashMap），修改返回的 Map 不会影响内部的缓存。</p>
     *
     * @return Map，Key 是 Topic 名称，Value 是 TopicMetadata
     */
    public Map<String, TopicMetadata> getTopicMetadata() {
        return new HashMap<>(topicMetadata);
    }

    /**
     * 获取指定 Topic 的元数据。
     *
     * @param topic Topic 名称
     * @return 该 Topic 的元数据；如果 Topic 不存在则返回 null
     */
    public TopicMetadata getTopicMetadata(String topic) {
        return topicMetadata.get(topic);
    }

    /**
     * 获取所有已知 Broker 的信息。
     * <p>返回的是一个副本（HashMap），修改返回的 Map 不会影响内部的缓存。</p>
     *
     * @return Map，Key 是 Broker ID，Value 是 BrokerInfo（主机和端口）
     */
    public Map<Integer, BrokerInfo> getBrokers() {
        return new HashMap<>(brokers);
    }

    // ==================== 内部数据类 ====================

    /**
     * Topic 元数据 —— 描述一个 Topic 的基本信息。
     *
     * <p>一个 Topic 的元数据主要就是它的分区列表。</p>
     */
    public static class TopicMetadata {
        /** Topic 名称 */
        private final String name;

        /** 该 Topic 下所有分区的信息列表 */
        private final List<PartitionInfo> partitions;

        /**
         * @param name       Topic 名称
         * @param partitions 分区信息列表
         */
        public TopicMetadata(String name, List<PartitionInfo> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        /**
         * 获取分区列表。
         * @return 分区信息的副本列表（防止外部修改内部数据）
         */
        public List<PartitionInfo> getPartitions() {
            return new ArrayList<>(partitions);
        }

        @Override
        public String toString() {
            return "TopicMetadata{name='" + name + "', partitions=" + partitions + "}";
        }
    }

    /**
     * 分区信息 —— 描述一个分区的状态。
     *
     * <h3>关键字段</h3>
     * <ul>
     *   <li><b>id</b>：分区编号，从 0 开始递增</li>
     *   <li><b>leader</b>：该分区 Leader 副本所在的 Broker ID。
     *       所有的读写操作都走 Leader</li>
     *   <li><b>followers</b>：Follower 副本所在的 Broker ID 列表。
     *       Follower 只做被动备份，不处理客户端请求</li>
     * </ul>
     */
    public static class PartitionInfo {
        /** 分区 ID，从 0 开始编号 */
        private final int id;

        /** Leader Broker 的 ID（负责该分区的所有读写操作） */
        private final int leader;

        /** Follower Broker 的 ID 列表（负责数据备份） */
        private final List<Integer> followers;

        /**
         * @param id        分区 ID
         * @param leader    Leader Broker ID
         * @param followers Follower Broker ID 列表
         */
        public PartitionInfo(int id, int leader, List<Integer> followers) {
            this.id = id;
            this.leader = leader;
            this.followers = followers;
        }

        public int getId() {
            return id;
        }

        /** @return 该分区的 Leader Broker ID */
        public int getLeader() {
            return leader;
        }

        /** @return Follower Broker ID 的副本列表 */
        public List<Integer> getFollowers() {
            return new ArrayList<>(followers);
        }

        @Override
        public String toString() {
            return "PartitionInfo{id=" + id + ", leader=" + leader + ", followers=" + followers + "}";
        }
    }
}
