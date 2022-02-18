package com.xiaomi.infra.pegasus.spark.analyser

import com.xiaomi.infra.pegasus.tools.Tools
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext

/**
  * Iterator of a pegasus partition. It's used to load the entire partition sequentially
  * and sorted by bytes-order.
  */
private[analyser] class PartitionIterator private (
    context: TaskContext,
    val pid: Int
) extends Iterator[PegasusRecord]
    with AutoCloseable {

  private val LOG = LogFactory.getLog(classOf[PartitionIterator])

  private var pegasusScanner: PegasusScanner = _
  private var filterExpiredRecord: Boolean = _

  private var closed = false
  private var nextRecord =
    PegasusRecord(
      null,
      null,
      null,
      0,
      0
    ) // the init value will be update by `next` and not be used

  private var name: String = _

  private var expiredCount: Long = 0
  private var totalCount: Long = 0

  def this(context: TaskContext, snapshotLoader: PegasusLoader, pid: Int) {
    this(context, pid)

    pegasusScanner = snapshotLoader.getScanner(pid)
    filterExpiredRecord = snapshotLoader.getConfig.isFilterExpired
    pegasusScanner.seekToFirst()
    assert(pegasusScanner.isValid)
    name = "PartitionIterator[pid=%d]".format(pid)
  }

  override def close() {
    if (!closed) {
      // release the C++ pointers
      pegasusScanner.close()
      closed = true
      LOG.info(
        toString() + " closed, filter=" + filterExpiredRecord + ", total=" + totalCount + ", expired=" + expiredCount
      )
    }
  }

  override def hasNext: Boolean = {
    updateNextRecord()
    if (filterExpiredRecord) {
      while (nextRecord != null && nextRecord.isExpired) {
        expiredCount += 1
        updateNextRecord()
      }
    }
    nextRecord != null && !closed
  }

  override def next(): PegasusRecord = {
    nextRecord
  }

  def updateNextRecord(): Unit = {
    pegasusScanner.next()
    if (pegasusScanner.isValid) {
      totalCount += 1
      nextRecord = pegasusScanner.restore()
    } else {
      nextRecord = null
    }
  }

  override def toString(): String = {
    name
  }
}
