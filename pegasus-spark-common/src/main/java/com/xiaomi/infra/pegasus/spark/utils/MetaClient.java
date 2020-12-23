package com.xiaomi.infra.pegasus.spark.utils;

import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.utils.MetaClient.TableInfo.GeneralInfo;
import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

public class MetaClient {
  // http table info resp format
  public static class TableInfo {
    public class GeneralInfo {
      public String app_name;
      public String app_id;
      public String partition_count;
      public String max_replica_count;
    }

    public GeneralInfo general;

    public TableInfo(GeneralInfo general) {
      this.general = general;
    }
  }

  private static String metaGateWay = "http://pegasus-gateway.hadoop.srv/";
  private static HttpClient httpClient;

  static {
    httpClient =
        HttpClientBuilder.create()
            .setRetryHandler(
                (exception, executionCount, context) ->
                    executionCount <= 5 && exception instanceof SocketException)
            .setServiceUnavailableRetryStrategy(
                new ServiceUnavailableRetryStrategy() {
                  public boolean retryRequest(
                      HttpResponse response, int executionCount, HttpContext context) {
                    return executionCount <= 5
                        && (response.getStatusLine().getStatusCode()
                                == HttpStatus.SC_SERVICE_UNAVAILABLE
                            || response.getStatusLine().getStatusCode()
                                == HttpStatus.SC_INTERNAL_SERVER_ERROR);
                  }

                  public long getRetryInterval() {
                    return 100;
                  }
                })
            .build();
  }

  public static TableInfo getTableInfo(String cluster, String table) throws PegasusSparkException {
    String path = String.format("/%s/meta/app", cluster);
    Map<String, String> params = new HashMap<>();
    params.put("name", table);

    HttpResponse httpResponse = get(path, params);
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

  private static HttpResponse get(String path, Map<String, String> params)
      throws PegasusSparkException {
    HttpGet request = new HttpGet(metaGateWay + path);
    List<NameValuePair> uriParams = parseParams(params);
    try {
      URI uri = new URIBuilder(request.getURI()).addParameters(uriParams).build();
      request.setURI(uri);
      return httpClient.execute(request);
    } catch (URISyntaxException e) {
      throw new PegasusSparkException(
          String.format("build get url failed, reason: %s", e.getMessage()));
    } catch (ParseException | IOException e) {
      throw new PegasusSparkException(
          String.format("get from %s failed, reason: %s", request.getURI(), e.getMessage()));
    } catch (Exception e) {
      throw new PegasusSparkException(String.format("get failed, reason: %s", e.getMessage()));
    }
  }

  private static List<NameValuePair> parseParams(Map<String, String> params) {
    List<NameValuePair> uriParams = new ArrayList<NameValuePair>();
    if (params != null) {
      for (Map.Entry<String, String> entry : params.entrySet()) {
        NameValuePair nvp = new BasicNameValuePair(entry.getKey(), entry.getValue());
        uriParams.add(nvp);
      }
    }
    return uriParams;
  }

  // TODO(jiashuo1)
  public HttpResponse post(String path, Map<String, String> params, String body) {
    HttpPost request = new HttpPost(metaGateWay + path);
    return null;
  }
}
