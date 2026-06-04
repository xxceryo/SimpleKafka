package com.loong.kafka.client;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SimpleKafka 生产者 —— 向 Kafka 发送消息的高级封装。
 *
 * <h2>什么是生产者（Producer）？</h2>
 * <p>生产者是向 Kafka 写入数据的应用程序。它负责：</p>
 * <ul>
 *   <li>决定消息发送到哪个 Topic</li>
 *   <li>决定消息发送到哪个分区（同一个 Topic 可能有多个分区）</li>
 *   <li>将消息序列化为字节数组，通过网络发送给 Broker</li>
 * </ul>
 *
 * <h2>本类的定位</h2>
 * <p>本类是对 {@link SimpleKafkaClient} 的封装，提供了更友好的接口：</p>
 * <ul>
 *   <li>自动管理 Topic 是否存在（不存在时可以自动创建）</li>
 *   <li>提供随机分区发送（负载均衡）和指定分区发送两种模式</li>
 *   <li>自动将字符串转换为 UTF-8 字节数组，无需手动处理编码</li>
 *   <li>内置 demo main 方法，可以直接运行体验</li>
 * </ul>
 *
 * <h2>快速使用示例</h2>
 * <pre>{@code
 * SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", 9092, "my-topic");
 * producer.initialize();
 * long offset = producer.send("Hello Kafka!");
 * producer.close();
 * }</pre>
 *
 * @see SimpleKafkaClient   底层通信客户端
 * @see SimpleKafkaConsumer 对应的消费者
 */
public class SimpleKafkaProducer {

    /** 日志记录器 */
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaProducer.class.getName());

    /** 创建 Topic 时默认的分区数量（3 个分区） */
    private static final int DEFAULT_PARTITIONS = 3;

    /** 创建 Topic 时默认的副本因子（每个分区 2 个副本，一主一备） */
    private static final short DEFAULT_REPLICATION = 2;

    /** 底层 Kafka 客户端，负责实际的网络通信 */
    private final SimpleKafkaClient client;

    /** 要发送到的目标 Topic 名称 */
    private final String topic;

    /**
     * 随机数生成器，用于随机选择分区。
     * 随机分区的好处是：当生产者发送多条消息时，消息会较均匀地分布到
     * 各个分区中，实现负载均衡。
     */
    private final Random random;

    /**
     * 是否在 Topic 不存在时自动创建。
     * 在开发/测试环境中设为 true 很方便，但在生产环境中通常应该提前创建好 Topic。
     */
    private final boolean createTopicIfNotExists;

    /**
     * 创建一个 SimpleKafka 生产者（Topic 不存在时自动创建）。
     *
     * @param bootstrapBroker 集群中任意 Broker 的主机名
     * @param bootstrapPort   Broker 的端口号
     * @param topic           目标 Topic 名称
     */
    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this(bootstrapBroker, bootstrapPort, topic, true);
    }

    /**
     * 创建一个 SimpleKafka 生产者（可控制是否自动创建 Topic）。
     *
     * @param bootstrapBroker        集群中任意 Broker 的主机名
     * @param bootstrapPort          Broker 的端口号
     * @param topic                  目标 Topic 名称
     * @param createTopicIfNotExists 设为 true 则在 Topic 不存在时自动创建
     */
    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic, boolean createTopicIfNotExists) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.random = new Random();
        this.createTopicIfNotExists = createTopicIfNotExists;
    }

    /**
     * 初始化生产者 —— 连接集群、检查/创建 Topic。
     *
     * <p>此方法必须在发送消息之前调用。它会：</p>
     * <ol>
     *   <li>连接 Bootstrap Broker，拉取集群元数据</li>
     *   <li>检查目标 Topic 是否存在</li>
     *   <li>如果 Topic 不存在且 createTopicIfNotExists=true，则自动创建</li>
     * </ol>
     *
     * @throws IOException 如果网络连接失败或 Topic 创建失败
     */
    public void initialize() throws IOException {
        client.initialize();

        // 检查目标 Topic 是否存在，如果配置了自动创建则创建它
        if (client.getTopicMetadata(topic) == null && createTopicIfNotExists) {
            LOGGER.info("Topic does not exist. Creating: " + topic);
            boolean created = client.createTopic(topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION);
            if (!created) {
                throw new IOException("Failed to create topic: " + topic);
            }
        }
    }

    /**
     * 发送一条消息到目标 Topic 的<b>随机分区</b>。
     *
     * <p>随机选择分区可以确保消息在多个分区之间均匀分布，
     * 避免所有消息都堆积在同一个分区中。</p>
     *
     * <p>如果需要精确控制消息发送到哪个分区（比如保证某些消息的顺序），
     * 请使用 {@link #send(String, int)} 方法。</p>
     *
     * @param message 消息内容（字符串格式）
     * @return Broker 分配的 Offset，可用于追踪消息位置
     * @throws IOException 如果发送失败
     */
    public long send(String message) throws IOException {
        SimpleKafkaClient.TopicMetadata metadata = client.getTopicMetadata(topic);
        if (metadata == null) {
            throw new IOException("Topic does not exist: " + topic);
        }

        // 获取分区总数，随机选一个分区
        int partitionCount = metadata.getPartitions().size();
        int partition = random.nextInt(partitionCount);

        return send(message, partition);
    }

    /**
     * 发送一条消息到目标 Topic 的<b>指定分区</b>。
     *
     * <h3>为什么需要指定分区？</h3>
     * <p>Kafka 保证同一分区内的消息是严格有序的（按发送顺序排列）。
     * 所以如果你有"消息 A 必须在消息 B 之前处理"这样的顺序要求，
     * 就需要把它们都发送到同一个分区。</p>
     *
     * @param message   消息内容（字符串格式）
     * @param partition 目标分区 ID（从 0 开始）
     * @return Broker 分配的 Offset
     * @throws IOException 如果发送失败或分区不存在
     */
    public long send(String message, int partition) throws IOException {
        // 将字符串转换为 UTF-8 编码的字节数组
        // 使用 UTF-8 是因为它是一种通用编码，能处理几乎所有语言的字符
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic, partition, data);
    }

    /**
     * 关闭生产者，释放资源。
     * <p>当前实现中 Socket 连接在每次发送后自动关闭，所以这里不需要做特殊清理。
     * 保留此方法是为了 API 的完整性，方便未来扩展（比如实现连接池）。</p>
     */
    public void close() {
        // No resources to close in this simple implementation
    }

    /**
     * 生产者的演示入口 —— 可以直接运行来体验 Kafka 消息发送。
     *
     * <h3>使用方法</h3>
     * <pre>java SimpleKafkaProducer localhost 9092 my-topic</pre>
     *
     * <p>程序会向指定 Topic 连续发送 10 条消息，每条消息间隔 1 秒。</p>
     *
     * @param args 命令行参数：[0]=broker主机, [1]=端口, [2]=topic名称
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaProducer <broker> <port> <topic>");
            System.exit(1);
        }

        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];

        try {
            SimpleKafkaProducer producer = new SimpleKafkaProducer(broker, port, topic);
            producer.initialize();

            System.out.println("Producer initialized. Sending 10 messages...");

            // 连续发送 10 条测试消息
            for (int i = 0; i < 10; i++) {
                // 每条消息包含序号和时间戳，方便观察
                String message = "Message " + i + " - " + System.currentTimeMillis();
                long offset = producer.send(message);
                System.out.println("Sent message to offset: " + offset);

                // 等待 1 秒再发送下一条，方便观察消费端的实时接收效果
                Thread.sleep(1000);
            }

            producer.close();
            System.out.println("Producer finished sending messages");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Producer error", e);
        }
    }
}
