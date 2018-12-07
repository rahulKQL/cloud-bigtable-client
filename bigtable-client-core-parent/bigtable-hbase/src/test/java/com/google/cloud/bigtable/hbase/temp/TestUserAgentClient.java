package com.google.cloud.bigtable.hbase.temp;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.InstanceName;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.BigtableDataSettingsFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.Channelz;
import io.grpc.internal.ServerImpl;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.ServerName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;

public class TestUserAgentClient {

  private static final String TEST_USER_AGENT = "sampleUserAgent";
  private Channelz channelz;
  final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName(TEST_PROJECT_ID, TEST_INSTANCE_ID).toTableName("fakeTable");
  private BigtableDataSettings dataSettings;
  private Server testServer;
  private static final AtomicReference<ServerCall<?, ?>> serverCallCapture =
      new AtomicReference<ServerCall<?, ?>>();
  private static final AtomicReference<Metadata> requestHeadersCapture =
      new AtomicReference<Metadata>();

  @Before
  public void setUp() throws IOException {

    List<ServerInterceptor> allInterceptors = ImmutableList.<ServerInterceptor>builder()
        .add(recordServerCallInterceptor(serverCallCapture))
        .add(recordRequestHeadersInterceptor(requestHeadersCapture))
        .add()
        .build();

    String uniqueName = InProcessServerBuilder.generateName();
    testServer = InProcessServerBuilder.forName(uniqueName)
        .directExecutor() // directExecutor is fine for unit tests
        .addService(ServerInterceptors.intercept(ServerServiceDefinition.builder("dataService").build(), allInterceptors))
        .build().start();
    channelz = new Channelz();
    channelz.addServer((ServerImpl)testServer);
    BigtableOptions bigtableOptions =
        BigtableOptions.builder()
            .setDataHost("localhost")
            .setAdminHost("localhost")
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setUserAgent(TEST_USER_AGENT)
            .setDataHost("127.0.0.1")
            .setPort(testServer.getPort()).build();
    dataSettings = BigtableDataSettingsFactory.fromBigtableOptions(bigtableOptions);

  }

  @After
  public void tearDown() throws Exception {
    if (testServer != null) {
      testServer.shutdownNow().awaitTermination();
    }
  }

  @Test
  public void testAdminMethod() throws InterruptedException {
    System.out.println(testServer);
    Futures.addCallback(channelz.getChannel(0).getStats(), new FutureCallback<Channelz.ChannelStats>() {
      @Override
      public void onSuccess(@Nullable Channelz.ChannelStats channelStats) {
        channelStats.
      }

      @Override
      public void onFailure(Throwable throwable) {

      }
    });
    System.out.println(channelz);
  }

  /**
   * Capture the request attributes. Useful for testing ServerCalls.
   * {@link ServerCall#getAttributes()}
   */
  public static ServerInterceptor recordServerCallInterceptor(
      final AtomicReference<ServerCall<?, ?>> serverCallCapture) {
    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call,
          Metadata requestHeaders,
          ServerCallHandler<ReqT, RespT> next) {
        serverCallCapture.set(call);
        return next.startCall(call, requestHeaders);
      }
    };
  }


  /**
   * Capture the request headers from a client. Useful for testing metadata propagation.
   */
  public static ServerInterceptor recordRequestHeadersInterceptor(
      final AtomicReference<Metadata> headersCapture) {
    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call,
          Metadata requestHeaders,
          ServerCallHandler<ReqT, RespT> next) {
        headersCapture.set(requestHeaders);
        return next.startCall(call, requestHeaders);
      }
    };
  }
}
