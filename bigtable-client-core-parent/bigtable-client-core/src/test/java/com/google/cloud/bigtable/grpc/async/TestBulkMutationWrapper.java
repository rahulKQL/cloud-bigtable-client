package com.google.cloud.bigtable.grpc.async;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.protobuf.ByteString;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestBulkMutationWrapper {

  final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName("project", "instance").toTableName("table");
  private final static ByteString QUALIFIER = ByteString.copyFrom("qual".getBytes());
  private final static int MAX_ROW_COUNT = 10;
  private final static BulkOptions BULK_OPTIONS = BulkOptions.builder()
      .setBulkMaxRequestSize(1000000L).setBulkMaxRowKeyCount(MAX_ROW_COUNT).build();

  private BulkMutation bulkMutation;

  private BigtableOptions options;

  private BulkMutationWrapper bulkMutationWrapper;

  @Before
  public void setUp(){
    bulkMutation = Mockito.mock(BulkMutation.class);
    options = BigtableOptions.builder()
        .setProjectId("fakeProjectId")
        .setInstanceId("fakeInstanceId")
        .setAppProfileId("fakeAppProfileId")
        .build();
    bulkMutationWrapper = new BulkMutationWrapper(bulkMutation, options);
  }
  @Test
  public void testIsFlush() throws InterruptedException, TimeoutException {
    Mockito.doNothing().when(bulkMutation).flush();
    bulkMutationWrapper.flush();
    Mockito.verify(bulkMutation).flush();
  }

  @Test
  public void testIsSendUnsent() throws InterruptedException, TimeoutException {
    Mockito.doNothing().when(bulkMutation).sendUnsent();
    bulkMutationWrapper.sendUnsent();
    Mockito.verify(bulkMutation).sendUnsent();
  }

  @Test
  public void testIsFlushed() throws InterruptedException, TimeoutException {
    Mockito.when(bulkMutation.isFlushed()).thenReturn(true);
    Assert.assertTrue(bulkMutationWrapper.isFlushed());
    Mockito.verify(bulkMutation).isFlushed();
  }
}
