package dev.caarlos0;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

public class StreamingJob {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.enableCheckpointing(10000);
    env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
    env.setStateBackend(new RocksDBStateBackend("file:///tmp/checkpoints"));
    env.setParallelism(1);
    env.setMaxParallelism(256);

    final RichSourceFunction<String> source =
        new RichSourceFunction<>() {
          private static final long serialVersionUID = 1L;
          private volatile boolean isRunning = true;

          @Override
          public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
              Thread.sleep(100L);
              ctx.collect(
                  "https://0c0ef67260ed959545b43995b7d85d4f.m.pipedream.net/" + UUID.randomUUID());
              LOG.info("emitted");
            }
            LOG.warn("not running");
          }

          @Override
          public void cancel() {
            LOG.info("cancel");
            this.isRunning = false;
          }
        };

    final SingleOutputStreamOperator<String> sourceStream = env.addSource(source).name("source");

    final SingleOutputStreamOperator<String> resultStream =
        AsyncDataStream.unorderedWait(
                sourceStream,
                new RichAsyncFunction<String, String>() {
                  private static final long serialVersionUID = 1L;

                  private transient CloseableHttpAsyncClient client;
                  private transient PoolingNHttpClientConnectionManager connectionManager;
                  private transient ConnectingIOReactor reactor;

                  @Override
                  public void open(Configuration parameters) throws Exception {
                    reactor = new DefaultConnectingIOReactor();
                    connectionManager = new PoolingNHttpClientConnectionManager(reactor);
                    connectionManager.setDefaultMaxPerRoute(100);
                    connectionManager.setMaxTotal(500);
                    client =
                        HttpAsyncClients.custom()
                            .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
                            .setConnectionReuseStrategy(DefaultConnectionReuseStrategy.INSTANCE)
                            .setConnectionManager(connectionManager)
                            .setDefaultRequestConfig(
                                RequestConfig.custom()
                                    .setConnectTimeout(60 * 1000)
                                    .setConnectionRequestTimeout(60 * 1000)
                                    .setSocketTimeout(60 * 1000)
                                    .build())
                            .build();
                    client.start();
                  }

                  @Override
                  public void close() throws Exception {
                    if (client != null && client.isRunning()) {
                      client.close();
                    }
                    if (connectionManager != null) {
                      connectionManager.shutdown();
                    }
                    if (reactor != null && reactor.getStatus() == IOReactorStatus.ACTIVE) {
                      reactor.shutdown();
                    }
                  }

                  @Override
                  public void asyncInvoke(String s, ResultFuture<String> resultFuture)
                      throws Exception {
                    final HttpUriRequest request =
                        RequestBuilder.get(s)
                            .addHeader(ACCEPT, APPLICATION_JSON.getMimeType())
                            .build();
                    final Future<HttpResponse> result = client.execute(request, null);
                    CompletableFuture.supplyAsync(
                            new Supplier<String>() {
                              @Override
                              public String get() {
                                try {
                                  final HttpResponse response = result.get();
                                  LOG.info("request completed: {}", s);
                                  return response.getStatusLine().toString();
                                } catch (ExecutionException | InterruptedException e) {
                                  LOG.error("failed: {}", s, e);
                                  return "NOPE";
                                }
                              }
                            })
                        .thenAccept(
                            status -> {
                              resultFuture.complete(Collections.singleton(status));
                              LOG.info("future completed: {} / {}", s, status);
                            });
                  }
                },
                10,
                TimeUnit.SECONDS)
            .name("async http");

    resultStream
        .addSink(
            new SinkFunction<String>() {
              @Override
              public void invoke(String value, Context context) throws Exception {
                LOG.info("sink: {}", value);
              }
            })
        .name("sink");

    env.execute("http");
  }
}
