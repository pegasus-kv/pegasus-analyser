package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.common.CommonConfig;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import java.io.Serializable;
import org.rocksdb.RocksDBException;

public interface PegasusLoader extends Serializable {

  int getPartitionCount();

  CommonConfig getConfig();

  PegasusScanner getScanner(int pid) throws PegasusSparkException, RocksDBException;
}
