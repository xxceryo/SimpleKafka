package com.loong.kafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Kafka 自定义通信协议
 * <p>
 * 协议格式：| 类型 (1字节) | 长度 (2字节) | 数据 (可变) |
 * - 类型：标识消息类型，见下方常量定义
 * - 长度：后续数据的字节数（不含类型和长度字段本身）
 * - 数据：实际载荷数据
 */
public class Protocol {

    // ==================== Client -> Broker 请求类型 ====================
    // 客户端发起的请求，使用 0x01-0x0F 范围

    /** 生产消息请求：客户端向 Broker 发送消息 */
    public static final byte PRODUCE = 0x01;

    /** 获取消息请求：客户端从 Broker 拉取消息 */
    public static final byte FETCH = 0x02;

    /** 元数据请求：客户端查询 Topic/Partition/Leader 等元信息 */
    public static final byte METADATA = 0x03;

    /** 创建主题请求：客户端请求创建新 Topic */
    public static final byte CREATE_TOPIC = 0x04;

    // ==================== Broker -> Client 响应类型 ====================
    // Broker 响应客户端的请求，使用 0x11-0x1F 范围

    /** 生产消息响应：对应 PRODUCE 请求 */
    public static final byte PRODUCE_RESPONSE = 0x11;

    /** 获取消息响应：对应 FETCH 请求 */
    public static final byte FETCH_RESPONSE = 0x12;

    /** 元数据响应：对应 METADATA 请求 */
    public static final byte METADATA_RESPONSE = 0x13;

    /** 创建主题响应：对应 CREATE_TOPIC 请求 */
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;

    /** 错误响应：通用错误响应，适用于任何请求类型出错的情况 */
    public static final byte ERROR_RESPONSE = 0x1F;

    // ==================== Broker 内部通信协议 ========================
    // Broker 集群内部节点间的通信，使用 0x21-0x2F 范围

    /** 消息复制请求：Leader 向 Follower 同步消息 */
    public static final byte REPLICATE = 0x21;

    /** 消息复制确认：Follower 确认已成功复制消息 */
    public static final byte REPLICATE_ACK = 0x22;

    /** 主题通知：Broker 间同步 Topic 配置变更等通知 */
    public static final byte TOPIC_NOTIFICATION = 0x23;


    // ==================== 协议实现方法 ==============================

    /**
     * 向客户端发送错误响应
     *
     * @param socketChannel 目标客户端的 Socket 通道
     * @param errorMessage   错误信息内容（UTF-8 编码）
     * @throws IOException 如果发生 I/O 错误
     */
    public static void sendErrorResponse(SocketChannel socketChannel, String errorMessage) throws IOException {
        // 分配缓冲区：1字节类型 + 2字节长度 + 消息内容
        ByteBuffer buffer = ByteBuffer.allocate(3 + errorMessage.length());
        // 写入响应类型
        buffer.put(ERROR_RESPONSE);
        // 写入消息长度（2字节 short）
        buffer.putShort((short) errorMessage.length());
        // 写入消息内容
        buffer.put(errorMessage.getBytes());
        // 切换为读模式
        buffer.flip();
        // 发送到客户端
        socketChannel.write(buffer);
    }

    /**
     * 编码生产消息请求
     *
     * @param topic     目标主题名称
     * @param partition 目标分区号
     * @param message   消息内容（字节数组）
     * @return 编码后的 ByteBuffer（已切换为读模式）
     */
    public static ByteBuffer encodeProducerRequest(String topic, int partition, byte[] message) {
        // 分配缓冲区：1(类型) + 2(topic长度) + topic内容 + 2(分区) + 2(消息长度) + 消息内容
        ByteBuffer buffer = ByteBuffer.allocate(11 + topic.length() + message.length);
        // 写入消息类型
        buffer.put(PRODUCE);
        // 写入 topic 长度和内容
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        // 写入分区号
        buffer.putShort((short) partition);
        // 写入消息长度和内容
        buffer.putShort((short) message.length);
        buffer.put(message);
        // 切换为读模式
        buffer.flip();
        return buffer;
    }

    /**
     * 编码获取消息请求
     *
     * @param topic     目标主题名称
     * @param partition 目标分区号
     * @param offset    起始偏移量（从该位置开始读取）
     * @param maxBytes  单次最大读取字节数
     * @return 编码后的 ByteBuffer（已切换为读模式）
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        // 分配缓冲区：1(类型) + 2(topic长度) + topic内容 + 4(分区) + 8(偏移量) + 4(最大字节数)
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length());
        // 写入消息类型
        buffer.put(FETCH);
        // 写入 topic 长度和内容
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        // 写入分区号
        buffer.putInt(partition);
        // 写入起始偏移量
        buffer.putLong(offset);
        // 写入单次最大读取字节数
        buffer.putInt(maxBytes);
        // 切换为读模式
        buffer.flip();
        return buffer;
    }

    /**
     * 编码元数据请求
     *
     * @return 编码后的 ByteBuffer（已切换为读模式）
     */
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    /**
     * 编码创建主题请求
     *
     * @param topic            主题名称（UTF-8 编码）
     * @param numPartitions    分区数量
     * @param replicationFactor 副本因子（复制因子）
     * @return 编码后的 ByteBuffer（已切换为读模式）
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        ByteBuffer buffer = ByteBuffer.allocate(9 + topic.length());
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    /**
     * 编码消息复制请求（Leader -> Follower）
     *
     * @param topic     目标主题名称
     * @param partition 目标分区号
     * @param offset    消息偏移量
     * @param message   消息内容（字节数组）
     * @return 编码后的 ByteBuffer（已切换为读模式）
     */
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(17 + topic.length() + message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * 编码主题通知消息（Broker 间广播）
     *
     * @param topic 目标主题名称
     * @return 编码后的 ByteBuffer（已切换为读模式）
     */
    public static ByteBuffer encodeTopicNotification(String topic) {
        ByteBuffer buffer = ByteBuffer.allocate(3 + topic.length());
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.flip();
        return buffer;
    }

    /**
     * 解码生产消息响应
     *
     * @param buffer 响应数据缓冲区
     * @return 生产结果，包含偏移量和错误信息
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new ProduceResult(-1, error);
            }
            return new ProduceResult(-1, "Invalid response type");
        }

        long offset = buffer.getLong();
        byte status = buffer.get();

        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }

    /**
     * 解码获取消息响应
     *
     * @param buffer 响应数据缓冲区
     * @return 获取结果，包含消息数组和错误信息
     */
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }

        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];

        for (int i = 0; i < messageCount; i++) {
            long offset = buffer.getLong(); // Skip offset
            int messageSize = buffer.getInt();
            messages[i] = new byte[messageSize];
            buffer.get(messages[i]);
        }

        return new FetchResult(messages, null);
    }

    /**
     * 解码元数据查询响应
     *
     * @param buffer 响应数据缓冲区
     * @return 元数据结果，包含 Broker 列表、Topic 列表和错误信息
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != METADATA_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(), error);
            }
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }

        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();

        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();
            short hostLength = buffer.getShort();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes);
            int port = buffer.getInt();

            brokers.add(new BrokerInfo(brokerId, host, port));
        }

        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();

        for (int i = 0; i < topicCount; i++) {
            short topicLength = buffer.getShort();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topicName = new String(topicBytes);

            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();

            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();

                int replicas = buffer.getInt();
                List<Integer> replicaIds = new ArrayList<>();

                for (int k = 0; k < replicas; k++) {
                    replicaIds.add(buffer.getInt());
                }

                partitions.add(new PartitionMetadata(partitionId, leaderId, replicaIds));
            }

            topics.add(new TopicMetadata(topicName, partitions));
        }

        return new MetadataResult(brokers, topics, null);
    }



    /**
     * Broker 节点信息
     */
    public static class BrokerInfo {
        private final int id;
        private final String host;
        private final int port;

        public BrokerInfo(int id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
        }

        public int getId() {
            return id;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }

    // ==================== 响应结果类 ==============================

    /**
     * 生产消息结果
     */
    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * 获取消息结果
     */
    public static class FetchResult {
        private final byte[][] messages;
        private final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public byte[][] getMessages() {
            return messages;
        }

        public int getMessageCount() {
            return messages.length;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * 元数据查询结果
     */
    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Topic 元数据信息
     */
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }

    /**
     * 分区元数据信息
     */
    public static class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }
    }
}
