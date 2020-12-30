package com.xiaomi.infra.pegasus.spark.utils.gateway;

import com.google.gson.reflect.TypeToken;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.utils.JsonParser;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

public class Cluster {

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

    return Integer.valueOf(version.keys().nextElement());
  }
}
