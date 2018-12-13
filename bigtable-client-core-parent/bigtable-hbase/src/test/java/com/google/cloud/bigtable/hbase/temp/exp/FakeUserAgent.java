package com.google.cloud.bigtable.hbase.temp.exp;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.BigtableDataSettingsFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;

public class FakeUserAgent {

  final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName(TEST_PROJECT_ID, TEST_INSTANCE_ID).toTableName("fakeTable");


  private Server server;

  private BigtableSession defaultSession;
  private BigtableSession profileSession;

  private BigtableDataSettings dataSettings;
  private BigtableDataClient dataClient;

  public void setUp() throws IOException {

    final int port;
    try(ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    server = ServerBuilder.forPort(port)
        .build();
    server.start();


    BigtableOptions opts = BigtableOptions.builder()
        .setDataHost("localhost")
        .setAdminHost("locahost")
        .setPort(port)
        .setProjectId("fake-project")
        .setInstanceId("fake-instance")
        .setUserAgent("fake-agent")
        .setUsePlaintextNegotiation(true)
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();

    dataSettings = BigtableDataSettingsFactory.fromBigtableOptions(opts);
    dataClient = BigtableDataClient.create(dataSettings);
    defaultSession = new BigtableSession(opts);

    profileSession = new BigtableSession(
        opts.toBuilder()
            .setAppProfileId("my-app-profile")
            .build()
    );
  }


  public void tearDown() throws Exception {
    if (defaultSession != null) {
      defaultSession.close();
    }

    if (profileSession != null) {
      profileSession.close();
    }

    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
  }

  public void testReadRows() throws Exception {
    defaultSession.getDataClient().readRows(ReadRowsRequest.getDefaultInstance()).next();

    profileSession.getDataClient().readRows(ReadRowsRequest.getDefaultInstance());
  }

  public void testNEw() throws Exception {
    Query query = Query.create(TABLE_NAME.getTableId());
    dataClient.readRows(query);
  }


  public static class UserAgentImpl extends BigtableGrpc.BigtableImplBase{

  }

  public static void main(String args[]) throws  Exception{
    RequestContext reqCon = RequestContext.create(InstanceName.of(TEST_PROJECT_ID,
        TEST_INSTANCE_ID), "appProfileId");
   
    System.out.println(query.toProto(reqCon));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(query);
    oos.flush();
    oos.close();

    InputStream is = new ByteArrayInputStream(baos.toByteArray());
    System.out.println(is);
    Thread.sleep(1000);
    System.out.println("des");
    ObjectInputStream ois = new ObjectInputStream(is);
    Query object = (Query)ois.readObject();
    System.out.println(object.toProto(reqCon));
    Assert.assertEquals(query.toProto(reqCon), object.toProto(reqCon));
  }
}
