/*
 * Copyright 2020 Google LLC.
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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@InternalApi("For internal usage only")
public class BigtableCoreConstants {

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String BIGTABLE_EMULATOR_HOST_ENV_VAR = "BIGTABLE_EMULATOR_HOST";

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String BIGTABLE_ADMIN_HOST_DEFAULT = "bigtableadmin.googleapis.com";

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String BIGTABLE_DATA_HOST_DEFAULT = "bigtable.googleapis.com";

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String BIGTABLE_BATCH_DATA_HOST_DEFAULT = "batch-bigtable.googleapis.com";

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final int BIGTABLE_PORT_DEFAULT = 443;

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final int BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT = getDefaultDataChannelCount();

  /**
   * Constant <code>BIGTABLE_APP_PROFILE_DEFAULT=""</code>, defaults to the server default app
   * profile
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final String BIGTABLE_APP_PROFILE_DEFAULT = "";

  /** @deprecated This field will be removed in future versions. */
  @Deprecated public static final String BIGTABLE_CLIENT_ADAPTER = "BIGTABLE_CLIENT_ADAPTER";

  private static int getDefaultDataChannelCount() {
    // 20 Channels seemed to work well on a 4 CPU machine, and this ratio seems to scale well for
    // higher CPU machines. Use no more than 250 Channels by default.
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    return Math.min(250, Math.max(1, availableProcessors * 4));
  }

  // ***************************** Bulk Operation *****************************

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final int BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT = 2;

  /**
   * This describes the maximum size a bulk mutation RPC should be before sending it to the server
   * and starting the next bulk call. Defaults to 1 MB.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final long BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT = 1 << 20;

  /**
   * This describes the maximum number of individual MutateRowsRequest.Entry objects to bundle in a
   * single bulk mutation RPC before sending it to the server and starting the next bulk call. The
   * server has a maximum of 100,000 total mutations.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT = 125;

  /**
   * Whether or not to enable a mechanism that reduces the likelihood that a {@link BulkMutation}
   * intensive application will overload a cluster.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final boolean BIGTABLE_BULK_ENABLE_THROTTLE_REBALANCE_DEFAULT = false;

  /**
   * The target RPC response time for a MutateRows request. This value is meaningful if bulk
   * mutation throttling is enabled. 100 ms. is a generally ok latency for MutateRows RPCs, but it
   * could go higher (for example 300 ms) for less latency sensitive applications that need more
   * throughput, or lower (10 ms) for latency sensitive applications.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT = 100;

  /**
   * The maximum amount of time a row will be buffered for. By default 0: indefinitely.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static long BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT = 0;

  /**
   * Default rpc count per channel.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT = 10;

  /**
   * This is the maximum accumulated size of uncompleted requests that we allow before throttling.
   * Default to 10% of available memory with a max of 1GB.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final long BIGTABLE_MAX_MEMORY_DEFAULT =
      (long) Math.min(1 << 30, (Runtime.getRuntime().maxMemory() * 0.1d));

  // ***************************** Rpc Timeouts *****************************

  /** Constant <code>USE_TIMEOUT_DEFAULT=false</code> */
  public static final boolean USE_TIMEOUT_DEFAULT = false;

  /**
   * The default duration to wait before timing out RPCs. 1 minute is probably too long for most
   * RPCs, but the intent is to have a conservative timeout by default and aim for user overrides.
   */
  public static final int SHORT_TIMEOUT_MS_DEFAULT = 60_000;

  /**
   * The default duration to wait before timing out RPCs. 10 minute is probably too long for most
   * RPCs, but the intent is to have a conservative timeout by default and aim for user overrides.
   * There could very well be 10 minute scans, so keep the value conservative for most cases and
   * allow user overrides as needed.
   */
  public static final int LONG_TIMEOUT_MS_DEFAULT = 600_000;

  // ***************************** Credentials Constants *****************************

  /** The OAuth scope required to perform administrator actions such as creating tables. */
  public static final String CLOUD_BIGTABLE_ADMIN_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.admin";
  /** The OAuth scope required to read data from tables. */
  public static final String CLOUD_BIGTABLE_READER_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.data.readonly";
  /** The OAuth scope required to write data to tables. */
  public static final String CLOUD_BIGTABLE_WRITER_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.data";

  /** Scopes required to read and write data from tables. */
  public static final List<String> CLOUD_BIGTABLE_READ_WRITE_SCOPES =
      ImmutableList.of(CLOUD_BIGTABLE_READER_SCOPE, CLOUD_BIGTABLE_WRITER_SCOPE);

  /** Scopes required for full access to cloud bigtable. */
  public static final List<String> CLOUD_BIGTABLE_ALL_SCOPES =
      ImmutableList.of(
          CLOUD_BIGTABLE_READER_SCOPE, CLOUD_BIGTABLE_WRITER_SCOPE, CLOUD_BIGTABLE_ADMIN_SCOPE);

  // ***************************** CredentialsOptions *****************************

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String SERVICE_ACCOUNT_JSON_ENV_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";

  // ***************************** RetryOptions *****************************

  /** @deprecated This field will be removed in the future */
  @Deprecated public static int DEFAULT_STREAMING_BUFFER_SIZE = 60;

  /**
   * Flag indicating whether or not grpc retries should be enabled. The default is to enable retries
   * on failed idempotent operations.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final boolean DEFAULT_ENABLE_GRPC_RETRIES = true;

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final Set<Status.Code> DEFAULT_ENABLE_GRPC_RETRIES_SET =
      ImmutableSet.of(
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.UNAVAILABLE,
          Status.Code.ABORTED,
          Status.Code.UNAUTHENTICATED);

  /**
   * We can timeout when reading large cells with a low value here. With a 10MB cell limit, 60
   * seconds allows our connection to drop to ~170kbyte/s. A 10 second timeout requires 1Mbyte/s
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS =
      (int) TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

  /**
   * Initial amount of time to wait before retrying failed operations (default value: 5ms).
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int DEFAULT_INITIAL_BACKOFF_MILLIS = 5;
  /**
   * Multiplier to apply to wait times after failed retries (default value: 1.5).
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final double DEFAULT_BACKOFF_MULTIPLIER = 1.5;
  /**
   * Maximum amount of time to retry before failing the operation (default value: 60 seconds).
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS =
      (int) TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);
  /**
   * Maximum number of times to retry after a scan timeout
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int DEFAULT_MAX_SCAN_TIMEOUT_RETRIES = 3;
}
