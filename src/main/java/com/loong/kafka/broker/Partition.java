package com.loong.kafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Kafka 分区（Partition）—— 消息存储的核心单位。
 *
 * <h3>什么是分区？</h3>
 * 在 Kafka 中，每个 Topic（主题）可以被分成多个 Partition（分区）。
 * 分区是一个<strong>有序、不可变</strong>的消息序列，每条消息在分区内都有一个唯一递增的编号，叫做 <strong>offset（偏移量）</strong>。
 *
 * <h3>分区如何存储消息？</h3>
 * 分区不会把所有消息写进一个巨大的文件，而是按大小切分成多个<strong>分段（Segment）</strong>。
 * 每个分段由两个文件组成：
 * <ul>
 *   <li><strong>.log 文件</strong>：存储消息的原始数据，按写入顺序追加。</li>
 *   <li><strong>.index 文件</strong>：索引文件，记录 offset 到 .log 文件物理位置的映射，方便快速查找。</li>
 * </ul>
 *
 * <h3>消息的磁盘格式</h3>
 * 每条消息的存储格式非常简单：
 * <pre>
 *   ┌──────────────┬──────────────────┐
 *   │ 消息长度(4字节) │   消息内容(N字节)  │
 *   └──────────────┴──────────────────┘
 * </pre>
 *
 * <h3>线程安全</h3>
 * 使用读写锁（ReadWriteLock）保证线程安全：多个消费者可以同时读取，但写入时会独占锁。
 *
 * <h3>Kafka 副本机制（简化版）</h3>
 * 每个分区有一个 Leader 副本和多个 Follower 副本：
 * <ul>
 *   <li>Leader：负责处理所有读写请求。</li>
 *   <li>Follower：从 Leader 同步数据，作为备份。</li>
 * </ul>
 */
public class Partition {

    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());

    /**
     * 单个分段文件的默认最大大小：1MB（1024 × 1024 字节）。
     * 当当前分段文件大小超过此值时，会自动创建新的分段文件。
     */
    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024; // 1MB

    /** .log 文件的后缀名，用于存储消息数据 */
    private static final String LOG_SUFFIX = ".log";

    /** .index 文件的后缀名，用于存储 offset → 文件位置 的映射 */
    private static final String INDEX_SUFFIX = ".index";

    /** 分区 ID（编号），在一个 Topic 内唯一 */
    private final int id;

    /** Leader 副本所在的 Broker ID */
    private int leader;

    /** Follower 副本所在的 Broker ID 列表 */
    private List<Integer> followers;

    /**
     * 分区数据文件存放的基础目录路径。
     * 例如："/data/kafka/topic-0" 表示 topic 的第 0 号分区的数据目录。
     */
    private final String baseDir;

    /**
     * 下一条消息应该分配的 offset（偏移量）。
     * 使用 AtomicLong 保证线程安全——多个线程同时写入时，offset 不会冲突。
     * 初始值为 0，每写入一条消息自动 +1。
     */
    private final AtomicLong nextOffset;

    /**
     * 读写锁，保证线程安全：
     * - 读锁（readLock）：允许多个线程同时读取消息（共享锁）
     * - 写锁（writeLock）：写入消息时独占，其他线程不能读也不能写（排他锁）
     */
    private final ReadWriteLock lock;

    /** 当前正在写入的活动日志文件句柄 */
    private RandomAccessFile activeLogFile;

    /** 当前活动日志文件的文件通道（NIO），用于高效的读写操作 */
    private FileChannel activeLogChannel;

    /** 所有分段（Segment）的元数据列表，按 baseOffset 从小到大排序 */
    private final List<SegmentInfo> segments;

    /**
     * 构造函数 —— 创建一个分区实例并初始化。
     *
     * @param id        分区 ID
     * @param leader    Leader 副本所在的 Broker ID
     * @param followers Follower 副本的 Broker ID 列表
     * @param baseDir   存储数据文件的根目录
     */
    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();

        // 初始化：创建目录、加载已有分段、或创建第一个分段
        initialize();
    }

    /**
     * 初始化分区 —— 启动时调用。
     * <p>
     * 流程：
     * <ol>
     *   <li>如果数据目录不存在，先创建它。</li>
     *   <li>扫描目录中已有的 .log 文件，恢复之前写入的分段信息。</li>
     *   <li>从最后一个分段的末尾恢复 nextOffset，确保 offset 连续不重复。</li>
     *   <li>如果没有任何分段（新分区），创建一个从 offset=0 开始的新分段。</li>
     * </ol>
     */
    private void initialize() {
        try {
            // 1. 确保数据目录存在，不存在则递归创建
            File dir = new File(baseDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // 2. 列出目录中所有的 .log 文件，恢复分段信息
            File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
            if (files != null && files.length > 0) {
                for (File file : files) {
                    // 文件名格式：00000000000000000000.log（20位数字）
                    // 去掉后缀，解析出 baseOffset
                    String baseName = file.getName().substring(0, file.getName().length() - LOG_SUFFIX.length());
                    long baseOffset = Long.parseLong(baseName);

                    // 检查对应的 .index 文件是否存在
                    File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
                    if (indexFile.exists()) {
                        SegmentInfo segment = new SegmentInfo(baseOffset, file.getAbsolutePath(), indexFile.getAbsolutePath());
                        segments.add(segment);
                    }
                }

                // 3. 将分段按 baseOffset 从小到大排序
                segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));

                // 4. 从最后一个分段恢复 nextOffset
                // 原理：计数最后一个分段里有多少条消息，加上该分段的 baseOffset 就是下一条消息的 offset
                if (!segments.isEmpty()) {
                    SegmentInfo lastSegment = segments.getLast();
                    nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment));
                }
            }

            // 5. 如果没有任何分段（新分区或目录为空），创建第一个分段
            if (segments.isEmpty()) {
                createNewSegment(0);
            } else {
                // 6. 打开最后一个分段，准备追加写入
                SegmentInfo lastSegment = segments.getLast();
                openSegmentForAppend(lastSegment);
            }

            LOGGER.info("分区 " + id + " 初始化完成，共 " + segments.size() + " 个分段，下一条 offset: " + nextOffset.get());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "分区 " + id + " 初始化失败", e);
        }
    }

    /**
     * 统计某个分段文件中包含了多少条消息。
     *
     * 原理：从头到尾遍历 .log 文件，每次读 4 字节（消息长度），
     * 然后跳过消息长度对应的字节数，计数器 +1，直到文件末尾。
     *
     * @param segment 要统计的分段信息
     * @return 分段中的消息总数
     * @throws IOException 如果读取文件失败
     */
    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0;
        // try-with-resources：Java 7 语法，() 里的资源会在 try 块结束后自动关闭
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
             FileChannel logChannel = logFile.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocate(4); // 只分配 4 字节，用于读消息长度

            // 只要当前文件位置还没到文件末尾，就继续读
            while (logChannel.position() < logChannel.size()) {
                buffer.clear(); // 重置 buffer：position=0, limit=capacity，准备写入
                int bytesRead = logChannel.read(buffer);
                if (bytesRead < 4) break; // 读不到完整的 4 字节说明到达末尾或文件损坏

                buffer.flip(); // 翻转 buffer：limit=position, position=0，准备读取
                int messageSize = buffer.getInt(); // 读出消息长度（4字节整数）

                // 跳过消息内容，position 直接向后移动 messageSize 个字节
                logChannel.position(logChannel.position() + messageSize);
                count++;
            }
        }

        return count;
    }

    /**
     * 创建一个新的分段（Segment）。
     *
     * 分段命名规则：baseOffset 用 20 位数字补零，例如：
     * - baseOffset=0    → 00000000000000000000.log 和 00000000000000000000.index
     * - baseOffset=100  → 00000000000000000100.log 和 00000000000000000100.index
     *
     * 这样设计的好处是文件名按字典序排列就是按 offset 排列，方便排序和查找。
     *
     * @param baseOffset 这个分段的第一条消息的 offset
     * @throws IOException 如果创建文件失败
     */
    private void createNewSegment(long baseOffset) throws IOException {
        // 用 20 位补零格式化 baseOffset 作为文件名前缀
        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX;

        // 创建 .log 文件
        File logFile = new File(logPath);
        logFile.createNewFile();

        // 创建 .index 文件
        File indexFile = new File(indexPath);
        indexFile.createNewFile();

        // 记录分段信息并加入列表
        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath);
        segments.add(segment);

        // 将这个新分段设为"当前活动分段"，后续写入都追加到它末尾
        openSegmentForAppend(segment);

        LOGGER.info("为分区 " + id + " 创建新分段，baseOffset: " + baseOffset);
    }

    /**
     * 打开指定分段，准备追加写入。
     *
     * 操作步骤：
     * <ol>
     *   <li>先关闭之前打开的活动分段（如果存在）。</li>
     *   <li>以"读写"模式打开目标分段的 .log 文件。</li>
     *   <li>将文件指针移动到文件末尾，后续写入就是追加。</li>
     * </ol>
     *
     * @param segment 要打开的分段信息
     * @throws IOException 如果打开文件失败
     */
    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // 关闭之前打开的活动分段
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }

        if (activeLogFile != null) {
            activeLogFile.close();
        }

        // 以"rw"（读写）模式打开文件，文件指针默认在开头
        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();

        // 将文件指针移动到末尾 —— 这样后续写入就是追加，不会覆盖已有数据
        activeLogChannel.position(activeLogChannel.size());
    }

    /**
     * 向分区追加（写入）一条消息。
     *
     * <h3>写入流程：</h3>
     * <ol>
     *   <li>获取写锁（独占锁），确保同一时间只有一个线程在写入。</li>
     *   <li>检查当前活动分段是否已满（超过 1MB），满了就创建新分段。</li>
     *   <li>构造消息的磁盘格式：4字节长度 + 消息内容。</li>
     *   <li>写入 .log 文件并强制刷盘（force）。</li>
     *   <li>更新 .index 索引文件。</li>
     *   <li>offset 自增 1，返回本次写入的 offset。</li>
     * </ol>
     *
     * @param message 消息的字节数组
     * @return 消息被分配到的 offset（如果失败返回 -1）
     */
    public long append(byte[] message) {
        lock.writeLock().lock(); // 获取写锁 —— 写入期间其他线程无法读写
        try {
            long currentOffset = nextOffset.get();


            // 检查是否需要"滚动"到新分段：如果当前文件位置已超过 1MB，创建新分段  当前为写后判断 会存在一条消息超限
            if (activeLogChannel.position() >= DEFAULT_SEGMENT_SIZE) {
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);
            }

            // 构造消息写入缓冲区：4字节（消息长度）+ 消息内容
            // ByteBuffer.allocate(n)：在 JVM 堆内存中分配 n 字节的缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length); // 先写入消息长度（4字节整数）
            buffer.put(message);           // 再写入消息内容
            buffer.flip();                  // 翻转：准备从缓冲区读数据写入文件

            // 获取当前文件写入位置（用来记录到索引中）
            long position = activeLogChannel.position();
            // 将缓冲区的数据写入文件通道
            activeLogChannel.write(buffer);

            // force(true)：强制将数据刷新到磁盘（包括文件元数据）
            // 这保证了消息的持久性——即使宕机也不会丢失
            activeLogChannel.force(true);

            // 更新索引：记录 offset → 文件位置 的映射
            updateIndex(currentOffset, position);

            // offset 自增，下一条消息会使用新的 offset
            nextOffset.incrementAndGet();

            return currentOffset; // 返回本次分配的 offset
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "向分区 " + id + " 追加消息失败", e);
            return -1;
        } finally {
            lock.writeLock().unlock(); // finally 中释放锁，确保异常时也不会死锁
        }
    }

    /**
     * 更新索引文件，记录一条 offset → 文件物理位置 的映射。
     *
     * <h3>索引文件格式：</h3>
     * 每条索引记录固定 16 字节：
     * <pre>
     *   ┌──────────────────┬──────────────────────┐
     *   │ offset (8字节)    │ 文件位置 position (8字节) │
     *   └──────────────────┴──────────────────────┘
     * </pre>
     *
     * 索引的作用：消费者从某个 offset 开始读消息时，不需要从头扫描整个 .log 文件，
     * 而是先查索引找到 offset 对应的文件位置，直接"跳到"那个位置开始读，大幅提升效率。
     *
     * @param offset   消息的逻辑偏移量
     * @param position 消息在 .log 文件中的物理字节位置
     */
    private void updateIndex(long offset, long position) {
        try {
            if (segments.isEmpty()) return;

            // 总是在最后一个分段（当前活动分段）的索引末尾追加记录
            SegmentInfo currentSegment = segments.getLast();

            // 打开索引文件，准备在末尾追加
            try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw");
                 FileChannel indexChannel = indexFile.getChannel()) {

                // 文件指针移到末尾，新记录追加到最后
                indexChannel.position(indexChannel.size());

                // 每条索引记录 16 字节：offset(8字节) + position(8字节)
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(offset);    // 写入 offset
                buffer.putLong(position);  // 写入对应的文件物理位置
                buffer.flip();

                indexChannel.write(buffer);
                indexChannel.force(true); // 强制刷盘，保证索引数据持久化
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "分区 " + id + " 更新索引失败", e);
        }
    }

    /**
     * 从指定 offset 开始读取消息。
     *
     * <h3>读取流程：</h3>
     * <ol>
     *   <li>通过二分查找定位目标 offset 所在的 .log 分段文件。</li>
     *   <li>通过索引文件找到 offset 对应的文件物理位置。</li>
     *   <li>从该位置开始，逐条读取消息，直到达到 maxBytes 限制或文件末尾。</li>
     *   <li>如果跨分段（读完一个分段的末尾还没到 maxBytes），自动跳到下一个分段继续读。</li>
     * </ol>
     *
     * @param offset   从哪个 offset 开始读（包含）
     * @param maxBytes 最多读取多少字节（包含消息长度字段的 4 字节）
     * @return 读取到的消息列表，每条消息是一个字节数组
     */
    public List<byte[]> readMessages(long offset, int maxBytes) {
        lock.readLock().lock(); // 获取读锁 —— 允许多个线程同时读
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0; // 已读取的字节数统计

        try {
            // 第一步：找到包含目标 offset 的分段
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if (targetSegment == null) {
                return messages; // offset 超出范围，返回空列表
            }

            // 第二步：通过索引找到 offset 对应的文件物理位置
            long position = findPositionForOffset(targetSegment, offset);
            if (position < 0) {
                return messages;
            }

            // 第三步：打开分段文件，从 position 位置开始顺序读取
            try (RandomAccessFile logFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                 FileChannel logChannel = logFile.getChannel()) {

                // 定位到目标位置
                logChannel.position(position);

                ByteBuffer sizeBuffer = ByteBuffer.allocate(4); // 用于读取每条消息的长度字段
                long currentOffset = offset; // 当前正在读取的 offset

                // 读取循环：两个终止条件——达到 maxBytes 限制 或 到达文件末尾
                while (bytesRead < maxBytes && logChannel.position() < logChannel.size()) {
                    // 读消息长度（4字节）
                    sizeBuffer.clear();
                    int sizeRead = logChannel.read(sizeBuffer);
                    if (sizeRead < 4) break; // 读不到完整的长度字段，文件可能结束

                    sizeBuffer.flip();
                    int messageSize = sizeBuffer.getInt(); // 消息体的长度

                    // 如果加上这条消息会超过 maxBytes 限制，就停止读取
                    if (bytesRead + messageSize > maxBytes) {
                        break;
                    }

                    // 根据消息长度读取消息内容
                    ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                    int messageRead = logChannel.read(messageBuffer);

                    // 如果读到的消息内容不完整（文件可能损坏），记录警告并停止
                    if (messageRead < messageSize) {
                        LOGGER.warning("在 offset " + currentOffset + " 处读取到不完整的消息");
                        break;
                    }

                    messageBuffer.flip();

                    // 从 ByteBuffer 中取出字节数组，加入结果列表
                    byte[] message = new byte[messageSize];
                    messageBuffer.get(message);
                    messages.add(message);

                    // 统计已读字节数：消息长度字段(4字节) + 消息内容(messageSize字节)
                    bytesRead += messageSize + 4;
                    currentOffset++;

                    // 第四步：跨分段处理
                    // 如果当前分段读完了，但还没达到 maxBytes，且还有下一条消息，
                    // 就打开下一个分段继续读
                    if (logChannel.position() >= logChannel.size() && currentOffset < nextOffset.get()) {
                        int nextSegmentIndex = segments.indexOf(targetSegment) + 1;
                        if (nextSegmentIndex < segments.size()) {
                            // 关闭当前分段
                            logChannel.close();
                            logFile.close();

                            // 切换到下一个分段
                            targetSegment = segments.get(nextSegmentIndex);
                            RandomAccessFile nextLogFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                            FileChannel nextLogChannel = nextLogFile.getChannel();

                            // 从下一个分段的开头开始读（position = 0）
                            position = 0;
                            nextLogChannel.position(position);
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "从分区 " + id + " 读取消息失败", e);
        } finally {
            lock.readLock().unlock();
        }

        return messages;
    }

    /**
     * 二分查找：确定给定 offset 落在哪个分段文件中。
     *
     * 原理：分段按 baseOffset 从小到大排列，通过二分查找可以快速定位。
     * 例如有 3 个分段：baseOffset=0, baseOffset=10, baseOffset=20
     * - offset=5  属于第 1 个分段（0 ~ 9）
     * - offset=15 属于第 2 个分段（10 ~ 19）
     * - offset=25 属于第 3 个分段（20 以上）
     *
     * @param offset 要查找的消息 offset
     * @return 包含该 offset 的分段信息，如果 offset 超出已有范围则返回 null
     */
    private SegmentInfo findSegmentForOffset(long offset) {
        // offset 超出了已有消息的范围
        if (segments.isEmpty() || offset >= nextOffset.get()) {
            return null;
        }

        int low = 0;
        int high = segments.size() - 1;

        while (low <= high) {
            int mid = (low + high) / 2;
            SegmentInfo segment = segments.get(mid);

            if (mid < segments.size() - 1) {
                // 不是最后一个分段：判断 offset 是否在 [当前分段的baseOffset, 下一个分段的baseOffset) 之间
                SegmentInfo nextSegment = segments.get(mid + 1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()) {
                    return segment;
                }
            } else {
                // 是最后一个分段：只要 offset >= baseOffset 就行（上界由 nextOffset 控制）
                if (offset >= segment.getBaseOffset()) {
                    return segment;
                }
            }

            // 缩小查找范围
            if (offset < segment.getBaseOffset()) {
                high = mid - 1; // offset 在左边一半
            } else {
                low = mid + 1;  // offset 在右边一半
            }
        }

        return null;
    }

    /**
     * 通过索引文件查找 offset 对应在 .log 文件中的物理位置。
     *
     * <h3>索引查找逻辑：</h3>
     * 索引文件中每条记录 16 字节，每条的 offset 记录的是相对于该分段 baseOffset 的偏移。
     *
     * 例如 baseOffset=10，要查 offset=15：
     * <ol>
     *   <li>计算相对偏移：relativeOffset = 15 - 10 = 5</li>
     *   <li>索引中第 5 条（从 0 开始）记录就是 offset=15 对应的文件位置</li>
     * </ol>
     *
     * @param segment 分段信息
     * @param offset  要查找的绝对 offset
     * @return 对应的文件物理位置（字节偏移），如果索引为空返回 0（从头开始）
     */
    private long findPositionForOffset(SegmentInfo segment, long offset) {
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath(), "r");
             FileChannel indexChannel = indexFile.getChannel()) {

            // 索引文件为空，说明这是新分段，从文件开头读
            if (indexChannel.size() == 0) {
                return 0;
            }

            // 计算相对偏移：该 offset 在这个分段内的索引编号
            long relativeOffset = offset - segment.getBaseOffset();

            // 每条索引记录 16 字节，计算一共有多少条记录
            long entryCount = indexChannel.size() / 16;

            if (relativeOffset >= entryCount) {
                // offset 超出了索引记录数，说明是末尾部分，取最后一条索引记录的位置
                indexChannel.position(indexChannel.size() - 16);
                ByteBuffer buffer = ByteBuffer.allocate(16);
                indexChannel.read(buffer);
                buffer.flip();

                buffer.getLong(); // 跳过 offset 字段（8字节）
                return buffer.getLong(); // 返回 position 字段（8字节）
            }

            // 正常情况下：定位到第 relativeOffset 条索引记录（每条 16 字节）
            indexChannel.position(relativeOffset * 16);
            ByteBuffer buffer = ByteBuffer.allocate(16);
            indexChannel.read(buffer);
            buffer.flip();

            buffer.getLong(); // 跳过 offset 字段
            return buffer.getLong(); // 返回 position 字段
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "查找 offset " + offset + " 的文件位置失败", e);
            return -1;
        }
    }

    // ======================== 基础 getter/setter 方法 ========================

    /** 获取分区 ID */
    public int getId() {
        return id;
    }

    /** 获取 Leader 副本所在的 Broker ID */
    public int getLeader() {
        return leader;
    }

    /** 设置 Leader 副本所在的 Broker ID（用于 Leader 切换） */
    public void setLeader(int leader) {
        this.leader = leader;
    }

    /** 获取 Follower 副本的 Broker ID 列表（返回副本防止外部修改） */
    public List<Integer> getFollowers() {
        return new ArrayList<>(followers);
    }

    /** 设置 Follower 副本的 Broker ID 列表（传入副本防止外部引用影响内部状态） */
    public void setFollowers(List<Integer> followers) {
        this.followers = new ArrayList<>(followers);
    }

    /**
     * 获取日志末尾的 offset（即下一条要写入的 offset）。
     * 在 Kafka 中叫做 LEO（Log End Offset）。
     *
     * @return 下一条消息将被分配的 offset
     */
    public long getLogEndOffset() {
        return nextOffset.get();
    }

    /**
     * 关闭分区，释放文件资源。
     *
     * 操作：
     * <ol>
     *   <li>获取写锁，防止关闭过程中有新的读写操作。</li>
     *   <li>关闭文件通道（FileChannel）。</li>
     *   <li>关闭文件句柄（RandomAccessFile）。</li>
     * </ol>
     */
    public void close() {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null && activeLogChannel.isOpen()) {
                activeLogChannel.close();
            }

            if (activeLogFile != null) {
                activeLogFile.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "关闭分区资源失败", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 分段元数据 —— 描述一个分段文件的基本信息。
     *
     * <h3>分段的结构：</h3>
     * 每个分段由 .log 和 .index 两个文件组成，通过 baseOffset 命名关联：
     * <pre>
     *   00000000000000000000.log   ← 消息数据
     *   00000000000000000000.index ← 索引（offset → 文件位置）
     * </pre>
     */
    private static class SegmentInfo {
        /**
         * 该分段中第一条消息的 offset。
         * 也是分段的唯一标识，用作文件名。
         */
        private final long baseOffset;

        /** .log 文件的绝对路径 */
        private final String logPath;

        /** .index 文件的绝对路径 */
        private final String indexPath;

        public SegmentInfo(long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public String getLogPath() {
            return logPath;
        }

        public String getIndexPath() {
            return indexPath;
        }
    }
}
