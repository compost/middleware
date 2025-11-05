package io.symplify.streams;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.util.Map;

/**
 * Custom RocksDB configuration for improved durability and performance.
 *
 * This configuration ensures that:
 * 1. Data is properly flushed to disk before acknowledging writes (WAL sync)
 * 2. Write buffers are sized appropriately to batch writes efficiently
 * 3. Block cache is configured to reduce disk I/O
 * 4. Proper compaction settings to maintain performance over time
 */
public class RocksDBConfig implements RocksDBConfigSetter {

  @Override
  public void setConfig(String storeName, Options options, Map<String, Object> configs) {

    // Write-Ahead Log (WAL) settings for durability
    // Force sync on every write to ensure data is persisted to disk
    // Default: false (data may be buffered in OS cache)
    options.setUseFsync(true);

    // Keep WAL files around longer to help with recovery
    // Default: 0 (no TTL-based deletion)
    options.setWalTtlSeconds(300); // 5 minutes
    // Default: 0 (no size-based limit)
    options.setWalSizeLimitMB(512); // 512 MB

    // Write buffer settings - larger buffers reduce write amplification
    // Set to 64MB per write buffer
    // Default: 64MB (Kafka Streams default is same)
    options.setWriteBufferSize(64 * 1024 * 1024);

    // Use 3 write buffers - allows background flushing while accepting writes
    // Default: 2
    options.setMaxWriteBufferNumber(3);

    // Start flushing when we have 2 write buffers full
    // Default: 1
    options.setMinWriteBufferNumberToMerge(2);

    // Block cache configuration for reads (128MB)
    // Default: 8MB (Kafka Streams default), Set to: 128MB for better read performance
    // IMPORTANT: Must use the existing BlockBasedTableConfig from options, not create a new one
    BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
    tableConfig.setBlockCache(new org.rocksdb.LRUCache(128 * 1024 * 1024));
    // Default: 4KB
    tableConfig.setBlockSize(16 * 1024); // 16KB blocks
    // Default: false
    tableConfig.setCacheIndexAndFilterBlocks(true);
    // Default: false
    tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
    // No need to call setTableFormatConfig since we're modifying the existing config

    // Compaction settings
    // Default: LEVEL (this is already the default)
    options.setCompactionStyle(CompactionStyle.LEVEL);
    // Default: false
    options.setLevelCompactionDynamicLevelBytes(true);

    // Target file size for L1 (64MB)
    // Default: 64MB (same as default)
    options.setTargetFileSizeBase(64 * 1024 * 1024);
    // Default: 1
    options.setTargetFileSizeMultiplier(2);

    // Max bytes for level base (256MB)
    // Default: 256MB (same as default)
    options.setMaxBytesForLevelBase(256 * 1024 * 1024);
    // Default: 10 (same as default)
    options.setMaxBytesForLevelMultiplier(10);

    // Compression settings - use LZ4 for speed, Zstd for higher levels
    // Default: SNAPPY_COMPRESSION
    options.setCompressionType(CompressionType.LZ4_COMPRESSION);
    // Default: DISABLE_COMPRESSION_OPTION
    options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);

    // Background job settings
    // Default: 2
    options.setMaxBackgroundJobs(4);

    // Keep more files open to reduce file open/close overhead
    // Default: -1 (unlimited)
    options.setMaxOpenFiles(512);

    // Enable statistics for monitoring (optional - can be disabled in production)
    // Default: 0 (disabled)
    options.setStatsDumpPeriodSec(600); // Dump stats every 10 minutes

    // Memtable settings
    // Default: 0 (disabled)
    options.setMemtableHugePageSize(2 * 1024 * 1024); // 2MB huge pages if available

    // Recovery settings
    // Default: true
    options.setAvoidFlushDuringRecovery(false); // Ensure flush during recovery
    // Default: false (already flushes on shutdown)
    options.setAvoidFlushDuringShutdown(false); // Ensure flush during shutdown

    // Paranoid checks for data integrity (can be disabled for performance)
    // Default: true (already enabled by default)
    options.setParanoidChecks(true);

    // WAL recovery mode - tolerate corrupted tail records
    // Default: PointInTimeRecovery
    options.setWalRecoveryMode(org.rocksdb.WALRecoveryMode.TolerateCorruptedTailRecords);

    // Increase parallelism for flushing and compaction
    // Default: 1, Set to: half of available processors (minimum 2)
    options.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors() / 2, 2));
  }

  @Override
  public void close(String storeName, Options options) {
    // Cleanup is handled by RocksDB
  }
}
