package com.google.cloud.bigtable.hbase.temp.service;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.common.collect.Queues;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;

public class UserAgentTest {

  final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName(TEST_PROJECT_ID, TEST_INSTANCE_ID).toTableName("fakeTable");


  private Server server;
  private FakeDataService fakeDataService;
  private BigtableDataClient dataClient;

  @Before
  public void setUp() throws IOException {
    fakeDataService = new FakeDataService();

    final int port;
    try(ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    server = ServerBuilder.forPort(port)
        .addService(fakeDataService)
        .build();
    server.start();


    ManagedChannelBuilder channelBuilder = ManagedChannelBuilder
        .forAddress("localhost", 8080)
        .userAgent("SomeUSerAgent")
        .usePlaintext();

    FixedTransportChannelProvider channel =
        FixedTransportChannelProvider.
            create(GrpcTransportChannel.
                create(channelBuilder.build()));

    BigtableDataSettings settings2 =
        BigtableDataSettings.newBuilder()
            .setEndpoint("localhost:"+ server.getPort())
            .setInstanceName(InstanceName.of(TEST_PROJECT_ID, TEST_INSTANCE_ID))
            .setTransportChannelProvider(channel)
            .build();

    dataClient = BigtableDataClient.create(settings2);
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
  }

  @Test
  public void testReadRows() throws Exception {
    Query query = Query.create(TABLE_NAME.getTableId());
    dataClient.readRows(query);
    Query returnedQuery = fakeDataService.popLastRequest();
    System.out.println(returnedQuery);
    Assert.assertNotNull(returnedQuery);
  }

  public static class FakeDataService extends DataSettingsV2Grpc.DataSettingsImplBase{
    final BlockingQueue<Object> requests = Queues.newLinkedBlockingDeque();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() throws InterruptedException {
      return (T)requests.poll(1, TimeUnit.SECONDS);
    }

    @Override
    public void readRows(Query request,
        StreamObserver<Row> responseObserver) {
      System.out.println("somss");
      requests.add(request);
      responseObserver.onCompleted();
    }
  }
}
