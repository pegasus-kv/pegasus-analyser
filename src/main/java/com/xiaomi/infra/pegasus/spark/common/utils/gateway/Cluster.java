package com.xiaomi.infra.pegasus.spark.common.utils.gateway;

import com.google.gson.reflect.TypeToken;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.utils.JsonParser;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

public class Cluster {
  private static final Log LOG = LogFactory.getLog(Cluster.class);

  public static String metaGateWay = "http://pegasus-gateway.hadoop.srv/";

  public static TableInfo getTableInfo(String cluster, String table) throws PegasusSparkException {
    String path = String.format(metaGateWay + "/%s/meta/app", cluster);
    Map<String, String> params = new HashMap<>();
    params.put("name", table);
    params.put("detail", "");

    HttpResponse httpResponse = HttpClient.get(path, params);
    int code = httpResponse.getStatusLine().getStatusCode();
    if (code != 200) {
      throw new PegasusSparkException(
          String.format(
              "get tableInfo[%s(%s)] from gateway failed, ErrCode = %d", cluster, table, code));
    }

    TableInfo tableInfo;
    try {
      String resp = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      tableInfo = JsonParser.getGson().fromJson(resp, TableInfo.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to tableInfo failed: %s", e.getMessage()));
    }
    return tableInfo;
  }

  public static int getTableVersion(String cluster, String table) throws PegasusSparkException {
    return getTableVersion(getTableInfo(cluster, table));
  }

  public static int getTableVersion(TableInfo tableInfo) throws PegasusSparkException {
    AtomicInteger replicaCount = new AtomicInteger();
    ConcurrentHashMap<String, String> version = new ConcurrentHashMap<>();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    Map<String, String> params = new HashMap<>();
    params.put("app_id", tableInfo.general.app_id);
    for (String node : tableInfo.nodes.keySet()) {
      if (node.equals("total")) {
        continue;
      }
      String path = String.format("http://%s/replica/data_version", node);
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                try {
                  HttpResponse httpResponse = HttpClient.get(path, params);
                  int code = httpResponse.getStatusLine().getStatusCode();
                  if (code != 200) {
                    throw new PegasusSparkException(
                        String.format(
                            "get table[%s(%s)] version from replica[%s] failed, ErrCode = %d",
                            tableInfo.general.app_name, tableInfo.general.app_id, node, code));
                  }
                  String resp = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                  Type type = new TypeToken<HashMap<String, ReplicaVersion>>() {}.getType();
                  Map<String, ReplicaVersion> replicaVersionMap =
                      JsonParser.getGson().fromJson(resp, type);
                  for (Map.Entry<String, ReplicaVersion> replica : replicaVersionMap.entrySet()) {
                    replicaCount.incrementAndGet();
                    version.putIfAbsent(
                        replica.getValue().data_version, replica.getValue().data_version);
                  }
                } catch (PegasusSparkException | IOException e) {
                  throw new RuntimeException(String.format("get table version failed:%s", e));
                }
              }));
    }

    CompletableFuture<Void> futureAll =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    futureAll.join();

    if (version.size() == 0) {
      throw new PegasusSparkException(
          String.format(
              "table[%s] version init failed, not allow to use bulkload!",
              tableInfo.general.app_name));
    }

    if (version.size() != 1) {
      throw new PegasusSparkException(
          String.format(
              "table[%s] has multi version, not allow to use " + "bulkload!",
              tableInfo.general.app_name));
    }

    if (replicaCount.get() != tableInfo.replicas.size() * 3) {
      throw new PegasusSparkException(
          String.format(
              "table[%s] has not enough replica(expect=%d, actual=%d), not allow to use bulkload!",
              tableInfo.general.app_name, tableInfo.replicas.size() * 3, replicaCount.get()));
    }

    return Integer.parseInt(version.keys().nextElement());
  }

  public static void startBackup(
      String cluster, String table, String remoteFileSystem, String remotePath)
      throws PegasusSparkException, InterruptedException {
    BackupInfo.ExecuteResponse executeResponse =
        Cluster.sendBackupRequest(cluster, table, remoteFileSystem, remotePath);
    if (!executeResponse.err.Errno.equals("ERR_OK")) {
      throw new PegasusSparkException(executeResponse.hint_message);
    }
    BackupInfo.QueryResponse queryResponse =
        Cluster.queryBackupResult(cluster, table, String.valueOf(executeResponse.backup_id));
    while (queryResponse.err.Errno.equals("ERR_OK")
        && queryResponse.backup_items.length == 1
        && queryResponse.backup_items[0].end_time_ms == 0) {
      if (queryResponse.backup_items[0].is_backup_failed) {
        throw new PegasusSparkException(
            String.format(
                "export %s.%s to %s %s failed, please check the pegasus server log",
                cluster, table, remoteFileSystem, remotePath));
      }

      LOG.warn(
          String.format(
              "export %s.%s to %s %s is running", cluster, table, remoteFileSystem, remotePath));
      Thread.sleep(10000);
      queryResponse =
          Cluster.queryBackupResult(cluster, table, String.valueOf(executeResponse.backup_id));
    }

    if (queryResponse.backup_items[0].end_time_ms == 0) {
      throw new PegasusSparkException(
          String.format(
              "export %s.%s to %s %s failed = [%s]%s, please check the pegasus server log",
              queryResponse.err.Errno,
              queryResponse.hint_message,
              cluster,
              table,
              remoteFileSystem,
              remotePath));
    }

    LOG.info(
        String.format(
            "export %s.%s to %s %s is completed", cluster, table, remoteFileSystem, remotePath));
  }

  private static BackupInfo.ExecuteResponse sendBackupRequest(
      String cluster, String table, String remoteFileSystem, String remotePath)
      throws PegasusSparkException {
    String path = String.format("%s/v1/backupManager/%s/backup", metaGateWay, "c4tst-function1");

    BackupInfo.ExecuteRequest executeRequest = new BackupInfo.ExecuteRequest();
    executeRequest.TableName = table;
    executeRequest.BackupProvider = remoteFileSystem;
    executeRequest.BackupPath = remotePath;
    HttpResponse httpResponse = HttpClient.post(path, JsonParser.getGson().toJson(executeRequest));

    int code = httpResponse.getStatusLine().getStatusCode();
    if (code != 200) {
      throw new PegasusSparkException(
          String.format(
              "start backup[%s(%s)] via gateway failed, ErrCode = %d", cluster, table, code));
    }
    BackupInfo.ExecuteResponse backupExecuteResponse;
    String respString = "";
    try {
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      backupExecuteResponse =
          JsonParser.getGson().fromJson(respString, BackupInfo.ExecuteResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to tableInfo failed: %s\n%s", e.getMessage(), respString));
    }
    return backupExecuteResponse;
  }

  private static BackupInfo.QueryResponse queryBackupResult(String cluster, String table, String id)
      throws PegasusSparkException {
    String path = String.format("%s/v1/backupManager/%s/%s/%s", metaGateWay, cluster, table, id);
    Map<String, String> params = new HashMap<>();
    HttpResponse httpResponse = HttpClient.get(path, params);

    int code = httpResponse.getStatusLine().getStatusCode();
    if (code != 200) {
      throw new PegasusSparkException(
          String.format(
              "query backup[%s(%s)] via gateway failed, ErrCode = %d", cluster, table, code));
    }

    BackupInfo.QueryResponse queryResponse;
    String respString = "";
    try {
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      queryResponse = JsonParser.getGson().fromJson(respString, BackupInfo.QueryResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to queryResponse failed: %s\n%s", e.getMessage(), respString));
    }
    return queryResponse;
  }

  public static void startBulkLoad(
      String clusterName, String tableName, String remoteFileSystem, String remotePath)
      throws InterruptedException, PegasusSparkException {

    BulkLoadInfo.QueryResponse queryResponse = queryBulkLoadResult(clusterName, tableName);
    if (queryResponse.app_status.contains("BLS_CANCEL")) {
      throw new PegasusSparkException("the last bulkload is " + queryResponse.app_status);
    }

    BulkLoadInfo.ExecuteResponse executeResponse =
        sendBulkLoadRequest(clusterName, tableName, remoteFileSystem, remotePath);
    while (!executeResponse.err.Errno.equals("ERR_OK")) {
      if (executeResponse.err.Errno.equals("ERR_BUSY")) {
        queryResponse = queryBulkLoadResult(clusterName, tableName);
        if (queryResponse.app_status.contains("BLS_CANCEL")) {
          throw new PegasusSparkException(
              String.format(
                  "last %s.%s bulkload is %s", clusterName, tableName, queryResponse.app_status));
        }
        LOG.info(
            String.format(
                "last bulkload[%s.%s] is running, process %s",
                clusterName, tableName, queryResponse.app_status));
        Thread.sleep(10000);
        executeResponse = sendBulkLoadRequest(clusterName, tableName, remoteFileSystem, remotePath);
        continue;
      }
      throw new PegasusSparkException(executeResponse.hint_message);
    }

    Thread.sleep(10000);
    queryResponse = queryBulkLoadResult(clusterName, tableName);
    if (!queryResponse.app_status.equals("BLS_DOWNLOADING")) {
      LOG.warn("the first stage should be BLS_DOWNLOADING, but now is " + queryResponse.app_status);
    }

    while (queryResponse.err.Errno.equals("ERR_OK")) {
      queryResponse = queryBulkLoadResult(clusterName, tableName);
      if (queryResponse.app_status.contains("BLS_CANCEL")) {
        throw new PegasusSparkException(
            String.format(
                "%s.%s bulkload is %s", clusterName, tableName, queryResponse.app_status));
      }

      if (queryResponse.app_status.equals("BLS_FAILED")) {
        throw new PegasusSparkException(
            String.format(
                "bulkload[%s.%s] failed. message = %s",
                clusterName, tableName, queryResponse.hint_message));
      }

      if (queryResponse.app_status.contains("BLS_SUCCEED")) {
        LOG.info(
            String.format(
                "bulkload[%s.%s] is completed, process %s",
                clusterName, tableName, queryResponse.app_status));
        return;
      }

      LOG.info(
          String.format(
              "bulkload[%s.%s] is running, process %s",
              clusterName, tableName, queryResponse.app_status));
      Thread.sleep(10000);
    }

    if (queryResponse.err.Errno.equals("ERR_INVALID_STATE")
        && queryResponse.hint_message.contains(" is not during bulk load")) {
      LOG.info(
          String.format(
              "bulkload[%s.%s] may be completed. message = %s",
              clusterName, tableName, queryResponse.hint_message));
    }
  }

  public static BulkLoadInfo.ExecuteResponse sendBulkLoadRequest(
      String clusterName, String tableName, String remoteFileSystem, String remotePath)
      throws PegasusSparkException {
    String path = String.format("%s/v1/bulkloadManager/start", metaGateWay);

    BulkLoadInfo.ExecuteRequest executeRequest = new BulkLoadInfo.ExecuteRequest();
    executeRequest.ClusterName = clusterName;
    executeRequest.TableName = tableName;
    executeRequest.RemoteProvider = remoteFileSystem;
    executeRequest.RemotePath = remotePath;
    HttpResponse httpResponse = HttpClient.post(path, JsonParser.getGson().toJson(executeRequest));

    int code = httpResponse.getStatusLine().getStatusCode();
    String error = httpResponse.getStatusLine().getReasonPhrase();
    if (code != 200) {
      throw new PegasusSparkException(
          String.format(
              "start bulkload[%s(%s)] via gateway[%s] failed, ErrCode = %d, error = %s",
              clusterName, tableName, path, code, error));
    }
    BulkLoadInfo.ExecuteResponse bulkloadExecuteResponse;
    String respString = "";
    try {
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      bulkloadExecuteResponse =
          JsonParser.getGson().fromJson(respString, BulkLoadInfo.ExecuteResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to tableInfo failed: %s\n%s", e.getMessage(), respString));
    }
    return bulkloadExecuteResponse;
  }

  public static BulkLoadInfo.QueryResponse queryBulkLoadResult(String clusterName, String tableName)
      throws PegasusSparkException {
    String path = String.format("%s/v1/bulkloadManager/%s/%s", metaGateWay, clusterName, tableName);
    Map<String, String> params = new HashMap<>();
    HttpResponse httpResponse = HttpClient.get(path, params);

    int code = httpResponse.getStatusLine().getStatusCode();
    String error = httpResponse.getStatusLine().getReasonPhrase();
    if (code != 200) {
      throw new PegasusSparkException(
          String.format(
              "query bulkload[%s(%s)] via gateway[%s] failed, ErrCode = %d, error = %s",
              clusterName, tableName, path, code, error));
    }

    BulkLoadInfo.QueryResponse queryResponse;
    String respString = "";
    try {
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      queryResponse = JsonParser.getGson().fromJson(respString, BulkLoadInfo.QueryResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      if (respString.contains("is not during bulk load")) {
        LOG.warn(String.format("%s.%s is not during bulk load", clusterName, tableName));
        BulkLoadInfo.QueryResponse response = new BulkLoadInfo.QueryResponse();
        response.err = new BulkLoadInfo.Error("ERR_OK");
        response.app_status = "BLS_INVALID";
        response.hint_message = e.getMessage();
        return response;
      } else {
        throw new PegasusSparkException(
            String.format(
                "parser the response to queryResponse failed: %s\n%s", e.getMessage(), respString));
      }
    }
    return queryResponse;
  }

  public static BulkLoadInfo.CancelResponse cancelBulkLoad(String clusterName, String tableName)
      throws PegasusSparkException {
    String path = String.format("%s/v1/bulkloadManager/cancel/", metaGateWay);
    BulkLoadInfo.CancelRequest cancelRequest = new BulkLoadInfo.CancelRequest();
    cancelRequest.ClusterName = clusterName;
    cancelRequest.TableName = tableName;
    HttpResponse httpResponse = HttpClient.post(path, JsonParser.getGson().toJson(cancelRequest));

    int code = httpResponse.getStatusLine().getStatusCode();
    String error = httpResponse.getStatusLine().getReasonPhrase();
    if (code != 200) {
      throw new PegasusSparkException(
          String.format(
              "cancel bulkload[%s(%s)] via gateway[%s] failed, ErrCode = %d, err = %s",
              clusterName, tableName, path, code, error));
    }

    BulkLoadInfo.CancelResponse cancelResponse;
    String respString = "";
    try {
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      cancelResponse = JsonParser.getGson().fromJson(respString, BulkLoadInfo.CancelResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to queryResponse failed: %s\n%s", e.getMessage(), respString));
    }
    return cancelResponse;
  }
}
