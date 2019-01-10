/*
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.config;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.stub.StreamObserver;
import java.net.ServerSocket;

import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.jvm.hotspot.utilities.AssertionFailure;

public class TestUserAgent {
  private static final Logger logger = new Logger(TestUserAgent.class);

  private static final String TEST_PROJECT_ID = "Project-Id";
  private static final String TEST_INSTANCE_ID = "instance";
  private static final String TEST_USER_AGENT = "sampleUserAgent";
  private static final Pattern EXPECTED_HEADER_PATTERN =
      Pattern.compile(BigtableVersionInfo.CORE_USER_AGENT + "," + TEST_USER_AGENT + ".*");

  private BigtableDataClient dataClient;
  private Server server;

  @Before
  public void setUp() throws Exception {
    //After fetching available port, closing the ServerSocket.
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();

    //Creating Server with ServerInterceptor, To fetch user-agent header.
    server = ServerBuilder.forPort(availablePort)
        .addService(ServerInterceptors.intercept(new BigtableExtendedImpl() {
        }, new HeaderServerInterceptor())).build();
    server.start();
    logger.info("Starting test server at port: " + availablePort);

    BigtableOptions bigtableOptions =
        BigtableOptions.builder()
            .setDataHost("localhost")
            .setAdminHost("localhost")
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setUserAgent(TEST_USER_AGENT)
            .setUsePlaintextNegotiation(true)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setPort(availablePort)
            .build();
    BigtableDataSettings dataSettings = BigtableVaneerSettingsFactory.createBigtableDataSettings(bigtableOptions);

    dataClient = BigtableDataClient.create(dataSettings);
  }

  @After
  public void tearDown() throws Exception {
    dataClient.close();
    if (server != null) {
      server.shutdown();
    }
    if (server != null) {
      server.awaitTermination();
    }
  }

  @Test
  public void testDataClient(){
    dataClient.readRow("my-table-name", "sample-row");
  }

  public static class BigtableExtendedImpl extends BigtableGrpc.BigtableImplBase {
    @Override
    public void mutateRow(MutateRowRequest request,
        StreamObserver<MutateRowResponse> responseObserver) {

      responseObserver.onNext(MutateRowResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void readRows(ReadRowsRequest request,
        StreamObserver<ReadRowsResponse> responseObserver) {
      responseObserver.onNext(ReadRowsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  public class HeaderServerInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        final Metadata requestHeaders,
        ServerCallHandler<ReqT, RespT> next) {
      //Logging all available headers.
      logger.info("header received from client:" + requestHeaders);

      Metadata.Key<String> USER_AGENT_KEY =
          Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER);
      String headerValue = requestHeaders.get(USER_AGENT_KEY);

      //In case of user-agent not matching, throwing AssertionFailure.
      if(!EXPECTED_HEADER_PATTERN.matcher(headerValue).matches()){
        throw new AssertionFailure("User-Agent's format did not match");
      }
      return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {}, requestHeaders);
    }
  }
}
