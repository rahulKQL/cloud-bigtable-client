/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.async;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestBulkMutationWrapper {

  private final ListeningExecutorService executor = MoreExecutors.
      listeningDecorator(Executors.newFixedThreadPool(5));
  private final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName("project", "instance").toTableName("table");

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
  public void testIsFlush() throws InterruptedException {
    Mockito.doNothing().when(bulkMutation).flush();
    bulkMutationWrapper.flush();
    Mockito.verify(bulkMutation).flush();
  }

  @Test
  public void testIsSendUnsent() {
    Mockito.doNothing().when(bulkMutation).sendUnsent();
    bulkMutationWrapper.sendUnsent();
    Mockito.verify(bulkMutation).sendUnsent();
  }

  @Test
  public void testIsFlushed() {
    Mockito.when(bulkMutation.isFlushed()).thenReturn(true);
    Assert.assertTrue(bulkMutationWrapper.isFlushed());
    Mockito.verify(bulkMutation).isFlushed();
  }

  @Test
  public void testAddWithRowMutation() {
    Callable<MutateRowResponse> mutationTask = new Callable<MutateRowResponse>() {
      @Override
      public MutateRowResponse call() throws Exception {
        return MutateRowResponse.getDefaultInstance();
      }
    };
    ListenableFuture<MutateRowResponse> expected = executor.submit(mutationTask);
    RowMutation rowMutation = RowMutation.create(TABLE_NAME.getTableId(), "fake-Row-Key");
    Mockito.when(bulkMutation.add(Mockito.any(MutateRowRequest.class))).thenReturn(expected);
    ListenableFuture<MutateRowResponse> actual = bulkMutationWrapper.add(rowMutation);

    Assert.assertEquals(expected, actual);
    Mockito.verify(bulkMutation).add(Mockito.any(MutateRowRequest.class));
  }
}
