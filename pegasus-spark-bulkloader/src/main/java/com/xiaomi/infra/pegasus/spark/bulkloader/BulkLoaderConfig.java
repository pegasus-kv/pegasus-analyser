package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.CommonConfig;
import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.FlowController.RateLimiterConfig;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;
import java.io.Serializable;

/**
 * The config used for generating the pegasus data which will be placed as follow":
 *
 * <p><DataPathRoot>/<ClusterName>/<TableName>
 * <DataPathRoot>/<ClusterName>/<TableName>/bulk_load_info => {JSON}
 * <DataPathRoot>/<ClusterName>/<TableName>/<PartitionIndex>/bulk_load_metadata => {JSON}
 * <DataPathRoot>/<ClusterName>/<TableName>/<PartitionIndex>/<FileIndex>.sst => RocksDB SST File
 */
public class BulkLoaderConfig extends CommonConfig {
  private static final String DATA_ROOT_PATH = "/pegasus-bulkloader";

  private AdvancedConfig advancedConfig;
  private String dataRootPath;
  private boolean autoDeletePreviousData;

  private int tableId;
  private int tablePartitionCount;

  public BulkLoaderConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    super(hdfsConfig, clusterName, tableName);
    initDefaultConfig();
  }

  public BulkLoaderConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    super(fdsConfig, clusterName, tableName);
    initDefaultConfig();
  }

  private void initDefaultConfig() {
    this.dataRootPath = DATA_ROOT_PATH;
    this.advancedConfig = new AdvancedConfig();
    this.autoDeletePreviousData = false;
  }

  /**
   * pegasus table ID
   *
   * <p>TODO(jiashuo): support automatically retrieval of the table ID of the specified table name
   *
   * @param tableId
   * @return this
   */
  public BulkLoaderConfig setTableId(int tableId) {
    this.tableId = tableId;
    return this;
  }

  /**
   * pegasus table partition count
   *
   * <p>TODO(jiashuo): support automatically retrieval of the partition count of the specified table
   * name
   *
   * @param tablePartitionCount
   * @return this
   */
  public BulkLoaderConfig setTablePartitionCount(int tablePartitionCount) {
    this.tablePartitionCount = tablePartitionCount;
    return this;
  }

  /**
   * set the bulkloader data root path, default is "/pegasus-bulkloader"
   *
   * @param dataRootPath data path root
   * @return this
   */
  public BulkLoaderConfig setDataRootPath(String dataRootPath) {
    this.dataRootPath = dataRootPath;
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
   * set RateLimiter config to control request flow, detail see {@link
   * com.xiaomi.infra.pegasus.spark.FlowController}
   *
   * @param rateLimiterConfig
   * @return this
   */
  @Override
  public BulkLoaderConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    super.setRateLimiterConfig(rateLimiterConfig);
    return this;
  }

  /**
   * whether to delete the previous same path bulkloader data when start load new data, if true, it
   * may take a long time to delete it firstly, otherwise, it will throw exceptions. detail see
   * {@link PegasusRecordRDD#checkExistAndTryDelete(BulkLoaderConfig)}
   *
   * @param autoDeletePreviousData
   */
  public BulkLoaderConfig enableAutoDeletePreviousData(boolean autoDeletePreviousData) {
    this.autoDeletePreviousData = autoDeletePreviousData;
    return this;
  }

  public String getDataRootPath() {
    return dataRootPath;
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

  public boolean isAutoDeletePreviousData() {
    return autoDeletePreviousData;
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
