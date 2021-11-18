package com.xiaomi.infra.pegasus.spark.utils.gateway;

import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;

public class HttpClient {

  private static final org.apache.http.client.HttpClient httpClient;
  private static final RequestConfig config;

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

    config =
        RequestConfig.custom()
            .setConnectTimeout(1000)
            .setConnectionRequestTimeout(1000)
            .setSocketTimeout(1000)
            .build();
  }

  public static HttpResponse get(String path, Map<String, String> params)
      throws PegasusSparkException {
    HttpGet request = new HttpGet(path);
    request.setConfig(config);
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

  public static HttpResponse post(String path, String jsonStr) throws PegasusSparkException {
    HttpPost request = new HttpPost(path);
    try {
      request.setHeader("Accept", "application/json");
      request.setHeader("Content-Type", "application/json");
      request.setEntity(new StringEntity(jsonStr, "UTF-8"));
      return httpClient.execute(request);
    } catch (ParseException | IOException e) {
      throw new PegasusSparkException(
          String.format("post to %s failed, reason: %s", request.getURI(), e.getMessage()));
    } catch (Exception e) {
      throw new PegasusSparkException(String.format("post failed, reason: %s", e.getMessage()));
    }
  }

  private static List<NameValuePair> parseParams(Map<String, String> params) {
    List<NameValuePair> uriParams = new ArrayList<>();
    if (params != null) {
      for (Map.Entry<String, String> entry : params.entrySet()) {
        NameValuePair nvp = new BasicNameValuePair(entry.getKey(), entry.getValue());
        uriParams.add(nvp);
      }
    }
    return uriParams;
  }
}
