package com.jada.processor

import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor
import org.jboss.logging.Logger

class GlobalStoreSupplier[K, V](val storeName: String)
    extends ProcessorSupplier[K, V, Void, Void] {

  override def get(): Processor[K, V, Void, Void] =
    new GlobalStoreUpdater(storeName)

}

class GlobalStoreUpdater[K, V](val storeName: String)
    extends Processor[K, V, Void, Void] {

  private final val logger =
    Logger.getLogger(classOf[GlobalStoreUpdater[_, _]])

  var store: KeyValueStore[K, V] = _

  override def process(record: api.Record[K, V]): Unit = {
    this.store.put(record.key(), record.value)
  }

  override def init(context: ProcessorContext[Void, Void]): Unit = {
    this.store = context.getStateStore(storeName)
  }

}
