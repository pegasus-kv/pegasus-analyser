package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.CommonConfig;
import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.utils.FlowController.RateLimiterConfig;
import com.xiaomi.infra.pegasus.spark.utils.gateway.Cluster;
import com.xiaomi.infra.pegasus.spark.utils.gateway.TableInfo;
import java.io.Serializable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The config used for generating the pegasus data which will be placed as follow":
 *
 * <p><DataPathRoot>/<ClusterName>/<TableName>
 * <DataPathRoot>/<ClusterName>/<TableName>/bulk_load_info => {JSON}
 * <DataPathRoot>/<ClusterName>/<TableName>/<PartitionIndex>/bulk_load_metadata => {JSON}
 * <DataPathRoot>/<ClusterName>/<TableName>/<PartitionIndex>/<FileIndex>.sst => RocksDB SST File
 */
public class BulkLoaderConfig extends CommonConfig {
  private static final Log LOG = LogFactory.getLog(BulkLoaderConfig.class);

  private String dataPathRoot = "/pegasus-bulkloader";
  private AdvancedConfig advancedConfig = new AdvancedConfig();

  private DataVersion tableDataVersion;
  private int tableId;
  private int tablePartitionCount;

  public BulkLoaderConfig(HDFSConfig hdfsConfig, String clusterName, String tableName)
      throws PegasusSparkException {
    super(hdfsConfig, clusterName, tableName);
    initTableInfo(); // table id, partitionCount, version are fetched via gateway by default.
    // Pegasus Server Version 2.2.0 required
  }

  public BulkLoaderConfig(FDSConfig fdsConfig, String clusterName, String tableName)
      throws PegasusSparkException {
    super(fdsConfig, clusterName, tableName);
    initTableInfo(); // table id, partitionCount, version are fetched via gateway by default.
    // Pegasus Server Version  2.2.0 required
  }

  /**
   * auto set table info from gateway{@link Cluster}
   *
   * @throws PegasusSparkException
   */
  public BulkLoaderConfig initTableInfo() throws PegasusSparkException {
    TableInfo tableInfo = Cluster.getTableInfo(getClusterName(), getTableName());
    setTableInfo(
        Integer.parseInt(tableInfo.general.app_id),
        Integer.parseInt(tableInfo.general.partition_count),
        Cluster.getTableVersion(tableInfo));
    LOG.info(
        "Init table info success:"
            + String.format(
                "cluster = %s, table = %s[%d(%d)], version = %s",
                getClusterName(),
                getTableName(),
                getTableId(),
                getTablePartitionCount(),
                getDataVersion().toString()));
    return this;
  }

  /**
   * pegasus table id, partitionCount and data version. if Pegasus Server Version >= 2.2.0, it can
   * be auto-init via {@linkplain BulkLoaderConfig#initTableInfo()}
   *
   * @param tableId
   * @param tablePartitionCount
   * @param dataVersion 0 or 1
   * @return
   */
  public BulkLoaderConfig setTableInfo(int tableId, int tablePartitionCount, int dataVersion)
      throws PegasusSparkException {
    this.tableId = tableId;
    this.tablePartitionCount = tablePartitionCount;
    switch (dataVersion) {
      case 0:
        this.tableDataVersion = new DataV0();
        break;
      case 1:
        this.tableDataVersion = new DataV1();
        break;
      default:
        throw new PegasusSparkException(
            String.format("Not support write data version: %d", dataVersion));
    }
    return this;
  }

  /**
   * set the bulkloader data root path, default is "/pegasus-bulkloader"
   *
   * @param dataPathRoot data path root
   * @return this
   */
  public BulkLoaderConfig setDataPathRoot(String dataPathRoot) {
    this.dataPathRoot = dataPathRoot;
    return this;
  }

  /**
   * set AdvancedConfig decide the data whether to sort or distinct, detail see {@link
   * AdvancedConfig}
   *
   * @param advancedConfig
   * @return this
   */
  public BulkLoaderConfig setAdvancedConfig(AdvancedConfig advancedConfig) {
    this.advancedConfig = advancedConfig;
    return this;
  }

  /**
   * set RateLimiter config to control request flow that include `qpsLimiter` and `bytesLimiter`,
   * detail see {@link com.xiaomi.infra.pegasus.spark.utils.FlowController} and {@link
   * RateLimiterConfig}
   *
   * @param rateLimiterConfig see {@link RateLimiterConfig}
   * @return this
   */
  @Override
  public BulkLoaderConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    super.setRateLimiterConfig(rateLimiterConfig);
    return this;
  }

  public String getDataPathRoot() {
    return dataPathRoot;
  }

  public DataVersion getDataVersion() {
    return tableDataVersion;
  }

  public int getTableId() {
    return tableId;
  }

  public int getTablePartitionCount() {
    return tablePartitionCount;
  }

  public AdvancedConfig getAdvancedConfig() {
    return advancedConfig;
  }

  /**
   * This class supports two options: enableSort and enableDistinct. Pegasus bulkload require the
   * data must be sorted and distinct by [hashKeyLength][hashKey][sortKey]. if you make sure that
   * the source data has been sorted or distinct base the rule, you can set them false to ignored
   * the sort or distinct process to decrease the time consuming. Otherwise, you may not should use
   * the class generally.
   */
  public static class AdvancedConfig implements Serializable {

    private boolean isDistinct = true;
    private boolean isSort = true;

    /**
     * set whether to distinct the [hashKeyLength][hashKey][sortKey] of pegasus records generated by
     * resource data, please make sure the data has been distinct base the above rule, otherwise,
     * don't set false.
     *
     * @param distinct true or false, default is "true"
     * @return this
     */
    public AdvancedConfig enableDistinct(boolean distinct) {
      isDistinct = distinct;
      return this;
    }

    /**
     * set whether to sort the [hashKeyLength][hashKey][sortKey] of pegasus records generated by
     * resource data, please make sure the data has been sorted base the above rule, otherwise,
     * don't set false.
     *
     * @param sort true or false, default is "true"
     * @return this
     */
    public AdvancedConfig enableSort(boolean sort) {
      isSort = sort;
      return this;
    }

    public boolean enableDistinct() {
      return isDistinct;
    }

    public boolean enableSort() {
      return isSort;
    }
  }
}
