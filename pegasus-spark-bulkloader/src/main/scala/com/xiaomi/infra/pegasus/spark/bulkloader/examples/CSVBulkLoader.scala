package com.xiaomi.infra.pegasus.spark.bulkloader.examples

import com.xiaomi.infra.pegasus.spark.CommonConfig.RemoteFSType
import com.xiaomi.infra.pegasus.spark.bulkloader.{
  BulkLoaderConfig,
  PegasusRecord
}
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._

object CSVBulkLoader {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("pegasus data bulkloader")
      .setIfMissing("spark.master", "local[1]")

    val sc = new SparkContext(conf)

    // Note: if the partition size > 2G before "saveAsPegasusFile", you need
    // sc.textFile("data.csv").repartition(n), and let the partition size < 2G
    sc.textFile("data.csv")
      .map(i => {
        val lines = i.split(",")
        PegasusRecord.createV1(
          lines(0).getBytes(),
          lines(1).getBytes(),
          lines(2).getBytes()
        )
      })
      .saveAsPegasusFile(
        BulkLoaderConfig.loadConfig(RemoteFSType.FDS)
      )
  }

}
