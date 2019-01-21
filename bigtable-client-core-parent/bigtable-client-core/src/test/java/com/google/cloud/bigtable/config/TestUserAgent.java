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

import com.google.api.gax.core.NoCredentialsProvider;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.common.io.Resources;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ForwardingServerCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.net.ServerSocket;

import java.net.URL;
import java.util.regex.Pattern;
import javax.net.ssl.SSLException;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static io.grpc.internal.GrpcUtil.USER_AGENT_KEY;

@RunWith(JUnit4.class)
public class TestUserAgent {
  private static final Logger logger = new Logger(TestUserAgent.class);

  private static final String TEST_PROJECT_ID = "Project-Id";
  private static final String TEST_INSTANCE_ID = "instance";
  private static final String TEST_USER_AGENT = "sampleUserAgent";
  private static final Pattern EXPECTED_HEADER_PATTERN =
      Pattern.compile(".*" + TEST_USER_AGENT + ".*");

  private BigtableDataSettings dataSettings;
  private BigtableDataClient dataClient;

  private Server server;

  /**
   * To Test UserAgent & PlainText Negotiation type
   * when {@link BigtableDataSettings} is created using {@link BigtableOptions}.
   */
  @Test
  public void testUserAgentUsingPlainTextNegotiation() throws Exception{
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();

    //Creates non-ssl server.
    createServer(availablePort);

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
    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(bigtableOptions);

    dataClient = BigtableDataClient.create(dataSettings);
    dataClient.readRow("my-table-name", "sample-row");
  }

  /**
   * To Test TLS Negotiation type using SSL enabled server
   * when {@link BigtableDataSettings} is created using {@link BigtableOptions}.
   */
  @Test
  public void testUserAgentUsingTLSNegotiation() throws Exception{
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();

    //Creates SSL enabled server.
    createSecuredServer(availablePort);

    BigtableDataSettings.Builder builder =
        BigtableDataSettings.newBuilder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setCredentialsProvider(NoCredentialsProvider.create());

    //Client Certificate
    SslContext sslContext  = buildSslContext();

    ManagedChannel sslChannel = NettyChannelBuilder
        .forAddress("localhost", availablePort)
        .sslContext(sslContext)
        .userAgent(TEST_USER_AGENT)
        .build();

    builder.setTransportChannelProvider(
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(sslChannel)));

    dataClient = BigtableDataClient.create(builder.build());
    dataClient.readRow("my-table-name", "sample-row");
  }

  /**
   * Ignoring this test cases as either of below scenario traps in infinite loop.
   * Created this test case for below two scenarios:
   * <pre>
   *   <ul>
   *     <li>When plainText is used on SSL-enabled server.</li>
   *     <li>When TLS Negotiation is used on non-SSL server.</li>
   *   </ul>
   * </pre>
   * @throws Exception
   */
  @Ignore
  @Test
  public void testPlainTextAndSSLServer() throws Exception{
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();
    createServer(availablePort);

    BigtableDataSettings.Builder builder =
        BigtableDataSettings.newBuilder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setCredentialsProvider(NoCredentialsProvider.create());
//    try{
//      HeaderProvider headers = FixedHeaderProvider.create(USER_AGENT_KEY.name(), TEST_USER_AGENT);
//      builder.setTransportChannelProvider(
//          InstantiatingGrpcChannelProvider.newBuilder()
//              .setHeaderProvider(headers)
//              .setEndpoint("localhost:"+availablePort)
//              .build());
//    }catch(Exception ex){
//      throw new AssertionError("could not create Channel" + ex.getMessage());
//    }

    //OR if we SSL-enabled server tries to use PlainText negotiation.

    try{
      //Client Certificate
      SslContext sslContext  = buildSslContext();

      ManagedChannel sslChannel = NettyChannelBuilder
          .forAddress("localhost", availablePort)
          .userAgent(TEST_USER_AGENT)
          .sslContext(sslContext)
          .usePlaintext()
          .build();

      builder.setTransportChannelProvider(
          FixedTransportChannelProvider.create(GrpcTransportChannel.create(sslChannel)));
    }catch(Exception ex){
      throw new AssertionError("could not create Channel" + ex.getMessage());
    }

    dataClient = BigtableDataClient.create(builder.build());
    dataClient.readRow("tableId", "rowkey");
  }

  //@After
  public void tearDown() throws Exception {
    dataClient.close();
    if (server != null) {
      server.shutdown();
    }
    if (server != null) {
      server.awaitTermination();
    }
  }

  private void createServer(int port) throws Exception{
    server = ServerBuilder.forPort(port)
        .addService(ServerInterceptors.intercept(new TestUserAgent.BigtableExtendedImpl() {
        }, new TestUserAgent.HeaderServerInterceptor()))
        .build();
    server.start();
  }

  private void createSecuredServer(int port) throws Exception{

    ServerBuilder builder = ServerBuilder.forPort(port)
        .addService(ServerInterceptors.intercept(new TestUserAgent.BigtableExtendedImpl() {},
            new TestUserAgent.HeaderServerInterceptor()));
    try{
      URL serverCertChain = Resources.getResource("sslCertificates/server.crt");
      URL privateKey = Resources.getResource("sslCertificates/server.pem");

      builder.useTransportSecurity(new File(serverCertChain.getFile()), new File(privateKey.getFile()));
    }catch(Exception ex){
      throw new AssertionError("No server certificates found");
    }
    server = builder.build();
    server.start();
  }

  private static SslContext buildSslContext() throws SSLException {
    SslContextBuilder builder = GrpcSslContexts.forClient();

    try{
      URL url = Resources.getResource("sslCertificates/ca.crt");
      if(url != null){
        builder.trustManager(new File(url.getFile()));
      }
    }catch(Exception ex){
      throw new AssertionError("No client trust certificate found");
    }
    return builder.build();
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

      //In case of user-agent not matching, throwing AssertionError.
      if(!EXPECTED_HEADER_PATTERN.matcher(headerValue).matches()){
        throw new AssertionError("User-Agent's format did not match");
      }
      return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {}, requestHeaders);
    }
  }
}
