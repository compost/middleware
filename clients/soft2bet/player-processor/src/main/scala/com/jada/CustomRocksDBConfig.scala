package com.jada

import org.apache.kafka.streams.state.RocksDBConfigSetter
import org.rocksdb.Options
import java.util

class CustomRocksDBConfig extends RocksDBConfigSetter {
  override def setConfig(
      storeName: String,
      options: Options,
      configs: util.Map[String, AnyRef]
  ): Unit = {
    // Increase threads for background work like flushing and compaction
    options.setIncreaseParallelism(8)
  }
}
