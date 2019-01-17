/*
 * Copyright 2018 Google LLC All Rights Reserved.
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
package com.google.cloud.bigtable.hbase2_x;

import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.longrunning.Operation;
import com.google.protobuf.Empty;

import java.util.concurrent.CompletableFuture;

import static com.google.cloud.bigtable.hbase2_x.FutureUtils.toCompletableFuture;

/**
 * A client for the Cloud Bigtable Table Admin API that uses {@link CompletableFuture}s instead of
 * {@link com.google.common.util.concurrent.ListenableFuture}
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableTableAdminClient {

  private final com.google.cloud.bigtable.grpc.BigtableTableAdminClient adminClient;
  private final IBigtableTableAdminClient adminClientWrapper;

  public BigtableTableAdminClient(
      com.google.cloud.bigtable.grpc.BigtableTableAdminClient adminClient,
      IBigtableTableAdminClient adminClientWrapper) {
    this.adminClient = adminClient;
    this.adminClientWrapper = adminClientWrapper;
  }

  /**
   * Creates a new table asynchronously. The table can be created with a full set of initial column
   * families, specified in the request.
   *
   * @param request a {@link CreateTableRequest} object.
   */
  public CompletableFuture<Table> createTableAsync(CreateTableRequest request) {
    return toCompletableFuture(adminClientWrapper.createTableAsync(request));
  }

  /**
   * Gets the details of a table asynchronously.
   *
   * @param tableId a {@link String} object.
   * @return a {@link CompletableFuture} that returns a {@link Table} object.
   */
  public CompletableFuture<Table> getTableAsync(String tableId) {
    return toCompletableFuture(adminClientWrapper.getTableAsync(tableId));
  }

  /**
   * Lists the names of all tables in an instance asynchronously.
   *
   * @param request a {@link ListTablesRequest} object.
   * @return a {@link CompletableFuture} that returns a {@link ListTablesResponse} object.
   */
  public CompletableFuture<ListTablesResponse> listTablesAsync(ListTablesRequest request) {
    return toCompletableFuture(adminClient.listTablesAsync(request));
  }

  /**
   * Permanently deletes a specified table and all of its data.
   *
   * @param request a {@link DeleteTableRequest} object.
   * @return a {@link CompletableFuture} that returns {@link Empty} object.
   */
  public CompletableFuture<Empty> deleteTableAsync(DeleteTableRequest request){
    return toCompletableFuture(adminClient.deleteTableAsync(request));
  }

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link ModifyColumnFamiliesRequest} object.
   * @return a {@link CompletableFuture} that returns {@link Table} object that contains the updated
   *         table structure.
   */
  public CompletableFuture<Table> modifyColumnFamilyAsync(ModifyColumnFamiliesRequest request) {
    return toCompletableFuture(adminClientWrapper.modifyFamiliesAsync(request));
  }

  /**
   * Permanently deletes all rows in a range.
   *
   * @param request a {@link DropRowRangeRequest} object.
   * @return a {@link CompletableFuture} that returns {@link Empty} object.
   */
  public CompletableFuture<Empty> dropRowRangeAsync(DropRowRangeRequest request) {
    return toCompletableFuture(adminClient.dropRowRangeAsync(request));
  }


  // ////////////// SNAPSHOT methods /////////////
  /**
   * Creates a new snapshot from a table in a specific cluster.
   * @param request a {@link SnapshotTableRequest} object.
   * @return The long running {@link Operation} for the request.
   */
  public CompletableFuture<Operation> snapshotTableAsync(SnapshotTableRequest request) {
    return toCompletableFuture(adminClient.snapshotTableAsync(request));
  }

  /**
   * Gets metadata information about the specified snapshot.
   * @param request a {@link GetSnapshotRequest} object.
   * @return The {@link Snapshot} definied by the request.
   */
  public CompletableFuture<Snapshot> getSnapshotAsync(GetSnapshotRequest request) {
    return toCompletableFuture(adminClient.getSnapshotAsync(request));
  }

  /**
   * Lists all snapshots associated with the specified cluster.
   * @param request a {@link ListSnapshotsRequest} object.
   * @return The {@link ListSnapshotsResponse} which has the list of the snapshots in the cluster.
   */
  public CompletableFuture<ListSnapshotsResponse> listSnapshotsAsync(ListSnapshotsRequest request) {
    return toCompletableFuture(adminClient.listSnapshotsAsync(request));
  }

  /**
   * Permanently deletes the specified snapshot.
   * @param request a {@link DeleteSnapshotRequest} object.
   */
  public CompletableFuture<Empty> deleteSnapshotAsync(DeleteSnapshotRequest request) {
    return toCompletableFuture(adminClient.deleteSnapshotAsync(request));
  }

  /**
   * Creates a new table from a snapshot.
   * @param request a {@link CreateTableFromSnapshotRequest} object.
   * @return The long running {@link Operation} for the request.
   */
  public CompletableFuture<Operation> createTableFromSnapshotAsync(CreateTableFromSnapshotRequest request) {
    return toCompletableFuture(adminClient.createTableFromSnapshotAsync(request));
  }
}
