package com.jada

import org.apache.kafka.streams.state.RocksDBConfigSetter
import org.rocksdb.{BlockBasedTableConfig, ChecksumType, Options}
import java.util

class CustomRocksDBConfig extends RocksDBConfigSetter {
  override def setConfig(
      storeName: String,
      options: Options,
      configs: util.Map[String, AnyRef]
  ): Unit = {
    // Increase threads for background work like flushing and compaction
    options.setIncreaseParallelism(8)

    // Disable paranoid checks
    options.setParanoidChecks(false)

    // Disable checksum verification (and generation for new blocks)
    val tableConfig = options.tableFormatConfig() match {
      case config: BlockBasedTableConfig => config
      case _                             => new BlockBasedTableConfig()
    }
    tableConfig.setChecksumType(ChecksumType.kNoChecksum)
    options.setTableFormatConfig(tableConfig)
  }
}
