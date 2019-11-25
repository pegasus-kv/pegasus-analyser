package com.xiaomi.infra.pegasus.analyser

import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.rocksdb.RocksDB

/**
  * PegasusContext is a serializable container for analysing Pegasus's checkpoint on HDFS.
  *
  * [[PegasusContext]] should be created in the driver, and shared with executors
  * as a serializable field.
  */
class PegasusContext(private val sc: SparkContext) extends Serializable {

  private val config = new Config("core-site.xml")

  def pegasusSnapshotRDD(clusterName: String,
                         tableName: String): PegasusSnapshotRDD = {
    new PegasusSnapshotRDD(this, clusterName, tableName, config, sc)
  }
}

/**
  * A RDD backed by a FDS snapshot of Pegasus.
  */
class PegasusSnapshotRDD private[analyser] (pegasusContext: PegasusContext,
                                            clusterName: String,
                                            tableName: String,
                                            config: Config,
                                            @transient sc: SparkContext)
    extends RDD[PegasusRecord](sc, Nil) {

  // Loads the librocksdb library into jvm.
  RocksDB.loadLibrary()

  private val LOG = LogFactory.getLog(classOf[PegasusSnapshotRDD])

  private val fdsService: FDSService =
    new FDSService(config, clusterName, tableName)

  override def compute(split: Partition,
                       context: TaskContext): Iterator[PegasusRecord] = {
    LOG.info(
      "Create iterator for \"%s\" \"%s\" [pid: %d]"
        .format(clusterName, tableName, split.index)
    )
    new PartitionIterator(context, config, fdsService, split.index)
  }

  override protected def getPartitions: Array[Partition] = {
    val indexes = Array.range(0, fdsService.getPartitionCount - 1)
    indexes.map(i => {
      new PegasusPartition(i)
    })
  }

  def getPartitionCount: Int = {
    fdsService.getPartitionCount
  }
}

/**
  * @param partitionIndex Each spark partition maps to one Pegasus partition.
  */
private class PegasusPartition(partitionIndex: Int) extends Partition {
  override def index: Int = partitionIndex
}
