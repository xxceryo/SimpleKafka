package com.loong.kafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SimpleKafka 消费者 —— 从 Kafka 读取消息的高级封装。
 *
 * <h2>什么是消费者（Consumer）？</h2>
 * <p>消费者是从 Kafka 读取数据的应用程序。它负责：</p>
 * <ul>
 *   <li>从指定 Topic 的指定分区拉取消息</li>
 *   <li>通过 Offset 追踪"读到哪里了"，确保不丢消息也不重复消费</li>
 *   <li>提供两种消费模式：手动单次拉取（poll）和自动循环拉取（startConsuming）</li>
 * </ul>
 *
 * <h2>两种消费模式</h2>
 *
 * <h3>1. 手动拉取模式（poll）</h3>
 * <p>你控制何时去拉取消息，适合将 Kafka 消费集成到自己的事件循环中：</p>
 * <pre>{@code
 * consumer.initialize();
 * List<byte[]> messages = consumer.poll();  // 拉取一批消息
 * for (byte[] msg : messages) {
 *     System.out.println(new String(msg, StandardCharsets.UTF_8));
 * }
 * }</pre>
 *
 * <h3>2. 自动回调模式（startConsuming）</h3>
 * <p>启动一个后台线程持续拉取，每当有新消息就回调你的处理方法：</p>
 * <pre>{@code
 * consumer.initialize();
 * consumer.startConsuming((message, offset) -> {
 *     System.out.println("收到消息: " + new String(message, StandardCharsets.UTF_8));
 * });
 * // 程序会持续消费，直到调用 stopConsuming()
 * }</pre>
 *
 * <h2>Offset 管理</h2>
 * <p>每次成功拉取消息后，本类会自动将 currentOffset 加上拉取到的消息数量。
 * 这意味着：如果你调用 poll() 拉到了 5 条消息，那么下次 poll() 会从
 * 第 6 条消息开始拉取，不会重复消费。</p>
 *
 * <p>也可以通过 {@link #seek(long)} 方法手动跳转到指定 Offset，
 * 比如想重新消费之前的消息。</p>
 *
 * @see SimpleKafkaClient   底层通信客户端
 * @see SimpleKafkaProducer 对应的生产者
 */
public class SimpleKafkaConsumer {

    /** 日志记录器 */
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());

    /**
     * 每次拉取消息的最大字节数（1MB）。
     * 这是一个限制值，防止一次拉取返回太多数据导致内存溢出。
     * 如果一批消息的大小超过这个值，Broker 可能会分批返回。
     */
    private static final int MAX_BYTES = 1024 * 1024;

    /**
     * 当没有新消息时，消费者等待多长时间（毫秒）再重试。
     * 这个"轮询间隔"是一个经典的权衡：
     * <ul>
     *   <li>间隔太短 → CPU 空转，浪费资源</li>
     *   <li>间隔太长 → 消息延迟大，不够实时</li>
     * </ul>
     * 100ms 是一个比较平衡的选择。
     */
    private static final int POLL_INTERVAL_MS = 100;

    /** 底层 Kafka 客户端，负责实际的网络通信 */
    private final SimpleKafkaClient client;

    /** 要消费的 Topic 名称 */
    private final String topic;

    /** 要消费的分区 ID */
    private final int partition;

    /**
     * 当前消费到的 Offset 位置。
     *
     * <p>这是消费者最核心的状态变量。它的含义是：</p>
     * <ul>
     *   <li>"已经消费了 Offset 在 [0, currentOffset) 范围内的所有消息"</li>
     *   <li>"下一次拉取将从 Offset = currentOffset 开始"</li>
     * </ul>
     *
     * <p>例如 currentOffset = 10，表示已消费了 0~9 号消息，
     * 下一次 poll() 将从第 10 号消息开始拉取。</p>
     */
    private long currentOffset;

    /**
     * 消费者是否正在运行的标志。
     *
     * <p>使用 AtomicBoolean 而不是普通 boolean 的原因是：
     * 消费者线程和主线程可能同时访问这个变量（一个读、一个写），
     * AtomicBoolean 保证了这个操作的原子性和可见性。</p>
     */
    private final AtomicBoolean running;

    /** 后台消费线程（仅在自动消费模式下使用） */
    private Thread consumerThread;

    /**
     * 创建一个消费者，从 Offset 0（分区的第一条消息）开始消费。
     *
     * @param bootstrapBroker 集群中任意 Broker 的主机名
     * @param bootstrapPort   Broker 的端口号
     * @param topic           要消费的 Topic 名称
     * @param partition       要消费的分区 ID
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this(bootstrapBroker, bootstrapPort, topic, partition, 0);
    }

    /**
     * 创建一个消费者，从指定的 Offset 开始消费。
     *
     * <h3>什么时候需要指定起始 Offset？</h3>
     * <ul>
     *   <li><b>从 0 开始</b>：消费分区的全部历史消息</li>
     *   <li><b>从最新 Offset 开始</b>：只消费新消息，跳过历史数据</li>
     *   <li><b>从某个特定 Offset 开始</b>：断点续传——上次程序挂了，
     *       记录下当时的 Offset，重启后从该位置继续</li>
     * </ul>
     *
     * @param bootstrapBroker 集群中任意 Broker 的主机名
     * @param bootstrapPort   Broker 的端口号
     * @param topic           要消费的 Topic 名称
     * @param partition       要消费的分区 ID
     * @param startOffset     起始 Offset（从这个位置开始读取）
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition, long startOffset) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startOffset;
        this.running = new AtomicBoolean(false);
    }

    /**
     * 初始化消费者 —— 连接集群并检查目标 Topic 是否存在。
     *
     * @throws IOException 如果连接失败或 Topic 不存在
     */
    public void initialize() throws IOException {
        client.initialize();

        if (client.getTopicMetadata(topic) == null) {
            throw new IOException("Topic does not exist: " + topic);
        }
    }

    /**
     * 手动跳转到指定的 Offset 位置。
     *
     * <p>调用此方法后，下一次 {@link #poll()} 将从该 Offset 开始拉取消息。
     * 这在以下场景中很有用：</p>
     * <ul>
     *   <li>重新消费已读过的消息（比如处理逻辑变了，需要重算）</li>
     *   <li>跳过一些损坏的消息</li>
     *   <li>跳转到最新位置，只消费新消息</li>
     * </ul>
     *
     * @param offset 目标 Offset 位置
     */
    public void seek(long offset) {
        this.currentOffset = offset;
    }

    /**
     * 手动拉取一次消息（单次操作，不会循环）。
     *
     * <h3>内部做了什么？</h3>
     * <ol>
     *   <li>调用 client.fetch() 从 Broker 拉取消息</li>
     *   <li>如果拉到了消息，自动将 currentOffset 加上消息数量</li>
     *   <li>返回拉取到的消息列表</li>
     * </ol>
     *
     * <p>如果没有新消息，返回空列表（不会阻塞，不会抛异常）。</p>
     *
     * @return 消息列表，每个元素是字节数组。空列表表示没有新消息
     * @throws IOException 如果网络通信失败
     */
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, MAX_BYTES);

        // 消费到消息后，自动推进 Offset
        // 这样下次 poll() 就不会重复返回同样的消息了
        if (!messages.isEmpty()) {
            currentOffset += messages.size();
        }
        return messages;
    }

    /**
     * 启动自动消费模式 —— 在后台线程中循环拉取消息，每收到消息就回调 handler。
     *
     * <h3>工作原理</h3>
     * <ol>
     *   <li>创建一个守护线程（Daemon Thread）</li>
     *   <li>在线程中不断调用 poll() 拉取消息</li>
     *   <li>每拉到一批消息，就逐条调用 handler.handle() 处理</li>
     *   <li>如果本次没有拉到消息，等待 {@link #POLL_INTERVAL_MS} 毫秒后再试</li>
     *   <li>直到 {@link #stopConsuming()} 被调用才退出循环</li>
     * </ol>
     *
     * <p><b>守护线程的含义</b>：如果主程序退出，后台线程会自动终止。
     * 这意味着即使忘记调用 stopConsuming()，程序也能正常退出。</p>
     *
     * <p><b>幂等性</b>：多次调用此方法不会创建多个线程（通过 running 标志检查）。</p>
     *
     * @param handler 消息处理器，每收到一条消息就回调它的 handle 方法
     * @see MessageHandler
     */
    public void startConsuming(MessageHandler handler) {
        // compareAndSet(false, true) 是原子操作：
        // "如果当前值是 false，就设为 true 并返回 true（表示切换成功）"
        // 这保证即使多个线程同时调用 startConsuming，也只会启动一个消费线程
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                try {
                    while (running.get()) {
                        // 拉取一批消息
                        List<byte[]> messages = poll();

                        // 逐条处理
                        for (byte[] message : messages) {
                            // 计算每条消息对应的 Offset
                            // currentOffset 已经在本轮 poll() 中推进了，所以需要回推
                            long messageOffset = currentOffset - messages.size() + messages.indexOf(message);
                            handler.handle(message, messageOffset);
                        }

                        // 如果没有新消息，等待一小段时间再试
                        // 避免空转浪费 CPU
                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (InterruptedException e) {
                    // 线程被中断是 stopConsuming() 正常触发的，不需要报错
                    // 但需要恢复中断状态（Java 线程中断规范）
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // 如果 running 还是 true，说明这不是因 stopConsuming 导致的异常
                    // 而是真正的错误，需要记录日志
                    if (running.get()) {
                        LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                    }
                    running.set(false);
                }
            });

            // 设为守护线程：主程序退出时自动终止，不会阻止 JVM 退出
            consumerThread.setDaemon(true);
            consumerThread.start();

            LOGGER.info("Started consuming from topic: " + topic + ", partition: " + partition);
        }
    }

    /**
     * 停止自动消费模式 —— 中断后台消费线程。
     *
     * <p><b>幂等性</b>：多次调用不会出错（通过 AtomicBoolean 检查）。</p>
     *
     * <p><b>停止过程</b>：</p>
     * <ol>
     *   <li>将 running 标志设为 false，通知消费线程退出循环</li>
     *   <li>中断消费线程（如果它正在 sleep 状态，中断会让它立即醒来）</li>
     *   <li>等待最多 1 秒让线程优雅退出</li>
     * </ol>
     */
    public void stopConsuming() {
        // "如果当前是 true，就设为 false 并返回 true"——只有从 true 切换到 false 才算成功
        if (running.compareAndSet(true, false)) {
            if (consumerThread != null) {
                try {
                    consumerThread.interrupt();      // 发送中断信号
                    consumerThread.join(1000);       // 等待线程结束（最多 1 秒）
                } catch (InterruptedException e) {
                    // join() 被中断了，恢复当前线程的中断状态
                    Thread.currentThread().interrupt();
                }
            }

            LOGGER.info("Stopped consuming from topic: " + topic + ", partition: " + partition);
        }
    }

    /**
     * 获取当前消费到的 Offset 位置。
     *
     * @return 当前 Offset（下次 poll 将从此位置开始拉取）
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    /**
     * 检查消费者是否正在运行（自动消费模式是否已经启动且未停止）。
     *
     * @return true 表示后台消费线程正在运行
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 关闭消费者 —— 停止消费并释放资源。
     */
    public void close() {
        stopConsuming();
    }

    // ==================== 内部接口 ====================

    /**
     * 消息处理器接口 —— 定义"如何处理消费到的消息"。
     *
     * <h3>为什么用接口（回调模式）？</h3>
     * <p>消费者负责"拉取"消息，但它不知道你想怎么处理消息（打印？存数据库？
     * 转发？计算？）。通过这个接口，你可以把"处理逻辑"注入到消费者中，
     * 消费者在拉取到消息后回调你的逻辑。</p>
     *
     * <p>这是一个函数式接口（只有一个抽象方法），可以用 Lambda 表达式实现：</p>
     * <pre>{@code
     * consumer.startConsuming((message, offset) -> {
     *     String text = new String(message, StandardCharsets.UTF_8);
     *     System.out.println("Offset " + offset + ": " + text);
     * });
     * }</pre>
     */
    @FunctionalInterface
    public interface MessageHandler {
        /**
         * 处理一条消息。
         *
         * @param message 消息内容（字节数组），可以使用
         *                {@code new String(message, StandardCharsets.UTF_8)} 转为字符串
         * @param offset  该消息在分区中的 Offset，可用于追踪和记录
         */
        void handle(byte[] message, long offset);
    }

    /**
     * 消费者的演示入口 —— 可以直接运行来体验 Kafka 消息消费。
     *
     * <h3>使用方法</h3>
     * <pre>java SimpleKafkaConsumer localhost 9092 my-topic 0</pre>
     *
     * <p>程序会持续消费指定 Topic/分区的消息，打印每条消息的内容和 Offset。
     * 按回车键即可停止消费并退出。</p>
     *
     * @param args 命令行参数：[0]=broker主机, [1]=端口, [2]=topic名称, [3]=分区ID
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: SimpleKafkaConsumer <broker> <port> <topic> <partition>");
            System.exit(1);
        }

        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);

        try {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(broker, port, topic, partition);
            consumer.initialize();

            System.out.println("Consumer initialized. Starting consumption...");

            // 启动自动消费模式：每收到消息就打印它的内容和 Offset
            consumer.startConsuming((message, offset) -> {
                String messageStr = new String(message, StandardCharsets.UTF_8);
                System.out.println("Received message at offset " + offset + ": " + messageStr);
            });

            System.out.println("Consumer started. Press enter to stop.");
            // 等待用户按回车键
            System.in.read();

            consumer.close();
            System.out.println("Consumer stopped");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Consumer error", e);
        }
    }
}
