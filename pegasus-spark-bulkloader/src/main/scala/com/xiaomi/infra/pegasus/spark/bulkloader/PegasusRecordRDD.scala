package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.spark.{JNILibraryLoader, PegasusSparkException}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

class PegasusRecordRDD(data: RDD[(PegasusKey, PegasusValue)]) {
  private val LOG = LogFactory.getLog(classOf[PegasusRecordRDD])

  def saveAsPegasusFile(config: BulkLoaderConfig): Unit = {
    checkExistAndTryDelete(config)

    var rdd = data
    if (config.getAdvancedConfig.enableDistinct) {
      rdd = rdd.reduceByKey((value1, value2) => value2)
    }

    if (config.getAdvancedConfig.enableSort) {
      rdd = rdd.repartitionAndSortWithinPartitions(
        new PegasusHashPartitioner(config.getTablePartitionCount)
      )
    } else {
      rdd = rdd.partitionBy(
        new PegasusHashPartitioner(config.getTablePartitionCount)
      )
    }

    rdd.foreachPartition(i => {
      JNILibraryLoader.load()
      new BulkLoader(config, i.asJava, TaskContext.getPartitionId()).start()
    })
  }

  // if has older bulkloader data, need delete it
  // TODO(jiashuo) the logic may need be deleted
  private def checkExistAndTryDelete(config: BulkLoaderConfig): Unit = {
    val tablePath = config.getRemoteFileSystemURL + "/" +
      config.getDataRootPath + "/" + config.getClusterName + "/" + config.getTableName
    val remoteFileSystem = config.getRemoteFileSystem

    if (remoteFileSystem.exist(tablePath)) {
      if (!config.isAutoDeletePreviousData) {
        throw new PegasusSparkException(
          "the data path [" + tablePath + "] has been existed!"
        )
      }

      LOG.warn(
        "the data path [" + tablePath + "] has been existed, and will be deleted!"
      )
      remoteFileSystem.delete(tablePath, true)
    }
  }

}
