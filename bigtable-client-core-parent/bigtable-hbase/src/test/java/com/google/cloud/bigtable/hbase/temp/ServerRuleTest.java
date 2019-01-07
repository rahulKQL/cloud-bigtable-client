package com.google.cloud.bigtable.hbase.temp;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.threeten.bp.Duration;

import static org.junit.Assert.assertTrue;

public class ServerRuleTest {

  private static final String PROJECT_ID = "fake-project";
  private static final String INSTANCE_ID = "fake-instance";
  private static final String TABLE_ID = "fake-table";

  private static final int MAX_ATTEMPTS = 5;
  private static final long FLUSH_COUNT = 10;
  private static final Duration FLUSH_PERIOD = Duration.ofMillis(50);
  private static final Duration DELAY_BUFFER = Duration.ofSeconds(1);

  @Rule public GrpcServerRule serverRule = new GrpcServerRule();
  private TestBigtableService service;
  private BulkMutationBatcher bulkMutations;
  private BigtableDataSettings.Builder settings;

  @Before
  public void setUp() throws Exception {
    service = new TestBigtableService();
    serverRule.getServiceRegistry().addService(service);

    settings =
        BigtableDataSettings.newBuilder()
            .setInstanceName(InstanceName.of(PROJECT_ID, INSTANCE_ID))
            .setCredentialsProvider(NoCredentialsProvider.create())
            .setTransportChannelProvider(
                FixedTransportChannelProvider.create(
                    GrpcTransportChannel.create(serverRule.getChannel())));

    settings
        .bulkMutationsSettings()
        .setRetrySettings(
            settings
                .bulkMutationsSettings()
                .getRetrySettings()
                .toBuilder()
                .setMaxAttempts(MAX_ATTEMPTS)
                .setInitialRetryDelay(Duration.ofMillis(10))
                .setRetryDelayMultiplier(2)
                .setMaxRetryDelay(Duration.ofMillis(100))
                .build())
        .setBatchingSettings(
            settings
                .bulkMutationsSettings()
                .getBatchingSettings()
                .toBuilder()
                .setDelayThreshold(FLUSH_PERIOD)
                .setElementCountThreshold(FLUSH_COUNT)
                .build());


    BigtableDataClient client = BigtableDataClient.create(settings.build());
    bulkMutations = client.newBulkMutationBatcher();

  }

  static class RpcExpectation {
    private final Map<String, Code> entries;
    private final Code resultCode;

    RpcExpectation(Code resultCode) {
      this.entries = Maps.newHashMap();
      this.resultCode = resultCode;
    }

    static RpcExpectation create() {
      return create(Code.OK);
    }

    static RpcExpectation create(Code resultCode) {
      return new RpcExpectation(resultCode);
    }

    RpcExpectation addEntry(String key, Code code) {
      entries.put(key, code);
      return this;
    }
  }

  static class TestBigtableService extends BigtableGrpc.BigtableImplBase {
    Queue<RpcExpectation> expectations = Queues.newArrayDeque();
    private final List<Throwable> errors = Lists.newArrayList();

    void verifyOk() {
      assertTrue(expectations.isEmpty());
      assertTrue(errors.isEmpty());
    }

    @Override
    public void mutateRows(
        MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
      try {
        mutateRowsUnsafe(request, responseObserver);
      } catch (Throwable t) {
        errors.add(t);
        throw t;
      }
    }

    private void mutateRowsUnsafe(
        MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
      RpcExpectation expectedRpc = expectations.poll();

      // Make sure that this isn't an extra request.
      System.out.println("request.toString");
      System.out.println(request.toString());

      // Make sure that this request has the same keys as the expected request.
      List<String> requestKeys = Lists.newArrayList();
      for (Entry entry : request.getEntriesList()) {
        requestKeys.add(entry.getRowKey().toStringUtf8());
      }

      Assert.assertNotNull(expectedRpc.entries.keySet());

      // Check if the expectation is to fail the entire request.
      if (expectedRpc.resultCode != Code.OK) {
        responseObserver.onError(Status.fromCode(expectedRpc.resultCode).asRuntimeException());
        return;
      }

      // Populate the response entries based on the set expectation.
      MutateRowsResponse.Builder responseBuilder = MutateRowsResponse.newBuilder();
      int i = 0;
      for (Entry requestEntry : request.getEntriesList()) {
        String key = requestEntry.getRowKey().toStringUtf8();
        Code responseCode = expectedRpc.entries.get(key);

        MutateRowsResponse.Entry responseEntry =
            MutateRowsResponse.Entry.newBuilder()
                .setIndex(i++)
                .setStatus(com.google.rpc.Status.newBuilder().setCode(responseCode.value()))
                .build();
        responseBuilder.addEntries(responseEntry);
      }

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }
  }

  @Test
  public void fullRequestRetryTest() {
    service.expectations.add(RpcExpectation.create(Code.DEADLINE_EXCEEDED).addEntry("key1", null));
    service.expectations.add(RpcExpectation.create().addEntry("key1", Code.OK));

    ApiFuture<Void> result = bulkMutations.add(RowMutation.create(TABLE_ID, "key1"));
    verifyOk(result);

    service.verifyOk();
  }

  @Test
  public void testDataSettings() throws Exception{
    System.out.println(settings);
    BigtableDataSettings settingsT = settings.build();

    System.out.println(settingsT);

    Assert.assertNotNull(settingsT.getTransportChannelProvider());
  }

  private void verifyOk(ApiFuture<?> result) {
    Throwable error = null;

    try {
      result.get(FLUSH_PERIOD.plus(DELAY_BUFFER).toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      error = e.getCause();
    } catch (Throwable t) {
      error = t;
    }

    Assert.assertNull(error);
  }
}
