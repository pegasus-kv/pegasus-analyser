package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.CommonConfig;
import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.utils.FlowController.RateLimiterConfig;
import com.xiaomi.infra.pegasus.spark.utils.gateway.Cluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ColdBackupConfig is used when you manipulate the cold-backup data. <br>
 * <br>
 * A pegasus cold-backup is a couple of well-organized files dumped from a pegasus table.<br>
 * It's a complete snapshot of the moment.
 */
public class ColdBackupConfig extends CommonConfig implements Config {
  private static final Log LOG = LogFactory.getLog(ColdBackupConfig.class);
  private static final DataType dataType = DataType.COLD_BACKUP;

  private static final long MB_UNIT = 1024 * 1024L;

  private static final int DEFAULT_FILE_OPEN_COUNT = 50;
  private static final long DEFAULT_READ_AHEAD_SIZE_MB = 1;

  private String backupID;
  private String rootPath = "/";
  private String policyName = "";
  private long readAheadSize;
  private int fileOpenCount;
  private String coldBackupTime;
  private DataVersion dataVersion;

  public ColdBackupConfig(
      HDFSConfig hdfsConfig,
      String rootPath,
      String backupID,
      String clusterName,
      String tableName) {
    super(hdfsConfig, clusterName, tableName);
    this.rootPath = rootPath;
    this.backupID = backupID;
    initConfig();
  }

  public ColdBackupConfig(
      FDSConfig fdsConfig, String rootPath, String backupID, String clusterName, String tableName) {
    super(fdsConfig, clusterName, tableName);
    this.rootPath = rootPath;
    this.backupID = backupID;
    initConfig();
  }

  public ColdBackupConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    super(hdfsConfig, clusterName, tableName);
    initConfig();
  }

  public ColdBackupConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    super(fdsConfig, clusterName, tableName);
    initConfig();
  }

  private void initConfig() {
    setReadOptions(DEFAULT_FILE_OPEN_COUNT, DEFAULT_READ_AHEAD_SIZE_MB);
  }

  /**
   * auto set data version from gateway{@link Cluster}
   *
   * @throws PegasusSparkException
   */
  public ColdBackupConfig initDataVersion() throws PegasusSparkException {
    int version = Cluster.getTableVersion(getClusterName(), getTableName());
    switch (version) {
      case 0:
        setDataVersion(new DataV0());
        break;
      case 1:
        setDataVersion(new DataV1());
        break;
      default:
        throw new PegasusSparkException(
            String.format("Not support read data version: %d", version));
    }

    LOG.info(
        "Init table version success:"
            + String.format(
                "cluster = %s, table = %s, version = %s",
                getClusterName(), getTableName(), getDataVersion().toString()));
    return this;
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  /**
   * cold backup policy name
   *
   * @param policyName policyName is pegasus server cold backup concept which is set when creating
   *     cold backup, see https://pegasus.apache.org/administration/cold-backup, here default is
   *     "every_day", you may need change it base your pegasus server config
   * @return this
   */
  public ColdBackupConfig setPolicyName(String policyName) {
    this.policyName = policyName;
    return this;
  }

  /**
   * cold backup creating time.
   *
   * @param coldBackupTime creating time of cold backup data, accurate to day level. for example:
   *     2019-09-11, default is null, means choose the latest data
   * @return this
   */
  public ColdBackupConfig setColdBackupTime(String coldBackupTime) {
    this.coldBackupTime = coldBackupTime;
    return this;
  }

  /**
   * pegasus data version
   *
   * @param dataVersion pegasus data has different data versions, default is {@linkplain DataV0}
   * @return this
   */
  public ColdBackupConfig setDataVersion(DataVersion dataVersion) {
    this.dataVersion = dataVersion;
    return this;
  }

  /**
   * @param maxFileOpenCount maxFileOpenCount is rocksdb concept which can control the max file open
   *     count, default is 50. detail see
   *     https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#general-options
   * @param readAheadSize readAheadSize is rocksdb concept which can control the readAheadSize,
   *     default is 1MB, detail see https://github.com/facebook/rocksdb/wiki/Iterator#read-ahead
   */
  public ColdBackupConfig setReadOptions(int maxFileOpenCount, long readAheadSize) {
    this.readAheadSize = readAheadSize * MB_UNIT;
    this.fileOpenCount = maxFileOpenCount;
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
  public ColdBackupConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    super.setRateLimiterConfig(rateLimiterConfig);
    return this;
  }

  public String getRootPath() {
    return rootPath;
  }

  public String getBackupID() {
    return backupID;
  }

  public long getReadAheadSize() {
    return readAheadSize;
  }

  public int getFileOpenCount() {
    return fileOpenCount;
  }

  public String getPolicyName() {
    return policyName;
  }

  public String getColdBackupTime() {
    return coldBackupTime;
  }

  public DataVersion getDataVersion() {
    return dataVersion;
  }
}
