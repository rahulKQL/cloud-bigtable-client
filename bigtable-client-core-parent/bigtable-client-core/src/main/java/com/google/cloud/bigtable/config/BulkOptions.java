/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import static com.google.cloud.bigtable.config.BigtableCoreConstants.BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableCoreConstants.BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableCoreConstants.BIGTABLE_BULK_ENABLE_THROTTLE_REBALANCE_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableCoreConstants.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableCoreConstants.BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableCoreConstants.BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableCoreConstants.BIGTABLE_MAX_MEMORY_DEFAULT;

import com.google.api.core.InternalExtensionOnly;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.io.Serializable;

/** An immutable class providing access to bulk related configuration options for Bigtable. */
@InternalExtensionOnly
public class BulkOptions implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  public static Builder builder() {
    return new Builder();
  }

  /** A mutable builder for BigtableConnectionOptions. */
  public static class Builder {

    private BulkOptions options;

    @Deprecated
    public Builder() {
      options = new BulkOptions();
      options.asyncMutatorCount = BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT;
      options.useBulkApi = true;
      options.bulkMaxRowKeyCount = BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT;
      options.bulkMaxRequestSize = BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT;
      options.autoflushMs = BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT;
      options.maxInflightRpcs = -1;
      options.maxMemory = BIGTABLE_MAX_MEMORY_DEFAULT;
      options.enableBulkMutationThrottling = BIGTABLE_BULK_ENABLE_THROTTLE_REBALANCE_DEFAULT;
      options.bulkMutationRpcTargetMs = BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT;
    }

    private Builder(BulkOptions options) {
      this.options = options.clone();
    }

    public Builder setAsyncMutatorWorkerCount(int asyncMutatorCount) {
      Preconditions.checkArgument(
          asyncMutatorCount >= 0, "asyncMutatorCount must be greater or equal to 0.");
      options.asyncMutatorCount = asyncMutatorCount;
      return this;
    }

    public Builder setUseBulkApi(boolean useBulkApi) {
      options.useBulkApi = useBulkApi;
      return this;
    }

    public Builder setBulkMaxRowKeyCount(int bulkMaxRowKeyCount) {
      Preconditions.checkArgument(
          bulkMaxRowKeyCount >= 0, "bulkMaxRowKeyCount must be greater or equal to 0.");
      options.bulkMaxRowKeyCount = bulkMaxRowKeyCount;
      return this;
    }

    public Builder setBulkMaxRequestSize(long bulkMaxRequestSize) {
      Preconditions.checkArgument(
          bulkMaxRequestSize >= 0, "bulkMaxRequestSize must be greater or equal to 0.");
      options.bulkMaxRequestSize = bulkMaxRequestSize;
      return this;
    }

    public Builder setAutoflushMs(long autoflushMs) {
      Preconditions.checkArgument(autoflushMs >= 0, "autoflushMs must be greater or equal to 0.");
      options.autoflushMs = autoflushMs;
      return this;
    }

    public Builder setMaxInflightRpcs(int maxInflightRpcs) {
      Preconditions.checkArgument(maxInflightRpcs > 0, "maxInflightRpcs must be greater than 0.");
      options.maxInflightRpcs = maxInflightRpcs;
      return this;
    }

    public Builder setMaxMemory(long maxMemory) {
      Preconditions.checkArgument(maxMemory > 0, "maxMemory must be greater than 0.");
      options.maxMemory = maxMemory;
      return this;
    }

    /**
     * Enable an experimental feature that will throttle requests from {@link BulkMutation} if
     * request latency surpasses a latency threshold. The default is {@link
     * BigtableCoreConstants#BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT}.
     *
     * @deprecated This will be removed in the future
     */
    @Deprecated
    public Builder enableBulkMutationThrottling() {
      options.enableBulkMutationThrottling = true;
      return this;
    }

    /** @deprecated This will be removed in the future */
    @Deprecated
    public Builder setBulkMutationRpcTargetMs(int bulkMutationRpcTargetMs) {
      options.bulkMutationRpcTargetMs = bulkMutationRpcTargetMs;
      return this;
    }

    public BulkOptions build() {
      return options;
    }
  }

  private int asyncMutatorCount;
  private boolean useBulkApi;
  private int bulkMaxRowKeyCount;
  private long bulkMaxRequestSize;
  private long autoflushMs;

  private int maxInflightRpcs;
  private long maxMemory;

  private boolean enableBulkMutationThrottling;
  private int bulkMutationRpcTargetMs;

  @VisibleForTesting
  BulkOptions() {
    asyncMutatorCount = 1;
    useBulkApi = false;
    bulkMaxRowKeyCount = -1;
    bulkMaxRequestSize = -1;
    autoflushMs = -1l;
    maxInflightRpcs = -1;
    maxMemory = -1l;
    enableBulkMutationThrottling = false;
    bulkMutationRpcTargetMs = -1;
  }

  private BulkOptions(
      int asyncMutatorCount,
      boolean useBulkApi,
      int bulkMaxKeyCount,
      long bulkMaxRequestSize,
      long autoflushMs,
      int maxInflightRpcs,
      long maxMemory,
      boolean enableBulkMutationThrottling,
      int bulkMutationRpcTargetMs) {
    this.asyncMutatorCount = asyncMutatorCount;
    this.useBulkApi = useBulkApi;
    this.bulkMaxRowKeyCount = bulkMaxKeyCount;
    this.bulkMaxRequestSize = bulkMaxRequestSize;
    this.autoflushMs = autoflushMs;
    this.maxInflightRpcs = maxInflightRpcs;
    this.maxMemory = maxMemory;
    this.enableBulkMutationThrottling = enableBulkMutationThrottling;
    this.bulkMutationRpcTargetMs = bulkMutationRpcTargetMs;
  }

  /**
   * Getter for the field <code>asyncMutatorCount</code>.
   *
   * @return a int.
   */
  public int getAsyncMutatorCount() {
    return asyncMutatorCount;
  }

  /**
   * useBulkApi.
   *
   * @return a boolean.
   */
  public boolean useBulkApi() {
    return useBulkApi;
  }

  /**
   * Getter for the field <code>bulkMaxRowKeyCount</code>.
   *
   * @return a int.
   */
  public int getBulkMaxRowKeyCount() {
    return bulkMaxRowKeyCount;
  }

  /**
   * Getter for the field <code>bulkMaxRequestSize</code>.
   *
   * @return a long.
   */
  public long getBulkMaxRequestSize() {
    return bulkMaxRequestSize;
  }

  /**
   * Getter for the field <code>autoflushMs</code>.
   *
   * @return a long
   */
  public long getAutoflushMs() {
    return autoflushMs;
  }

  /**
   * Getter for the field <code>maxInflightRpcs</code>.
   *
   * @return a int.
   */
  public int getMaxInflightRpcs() {
    return maxInflightRpcs;
  }

  /**
   * Getter for the field <code>maxMemory</code>.
   *
   * @return a long.
   */
  public long getMaxMemory() {
    return maxMemory;
  }

  /**
   * Is an experimental feature of throttling bulk mutation RPCs turned on?
   *
   * @return a boolean
   */
  public boolean isEnableBulkMutationThrottling() {
    return enableBulkMutationThrottling;
  }

  /**
   * if {@link #isEnableBulkMutationThrottling()}, then bulk mutation RPC latency will be compared
   * against this value. If the RPC latency is higher, then some throttling will be applied.
   *
   * @return the number of milliseconds that is an appropriate amount of time for a bulk mutation
   *     RPC.
   */
  public int getBulkMutationRpcTargetMs() {
    return bulkMutationRpcTargetMs;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != BulkOptions.class) {
      return false;
    }
    BulkOptions other = (BulkOptions) obj;
    return (asyncMutatorCount == other.asyncMutatorCount)
        && (useBulkApi == other.useBulkApi)
        && (bulkMaxRowKeyCount == other.bulkMaxRowKeyCount)
        && (bulkMaxRequestSize == other.bulkMaxRequestSize)
        && (autoflushMs == other.autoflushMs)
        && (maxInflightRpcs == other.maxInflightRpcs)
        && (maxMemory == other.maxMemory)
        && (enableBulkMutationThrottling == other.enableBulkMutationThrottling)
        && (bulkMutationRpcTargetMs == other.bulkMutationRpcTargetMs);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("asyncMutatorCount", asyncMutatorCount)
        .add("useBulkApi", useBulkApi)
        .add("bulkMaxKeyCount", bulkMaxRowKeyCount)
        .add("bulkMaxRequestSize", bulkMaxRequestSize)
        .add("autoflushMs", autoflushMs)
        .add("maxInflightRpcs", maxInflightRpcs)
        .add("maxMemory", maxMemory)
        .add("enableBulkMutationThrottling", enableBulkMutationThrottling)
        .add("bulkMutationRpcTargetMs", bulkMutationRpcTargetMs)
        .toString();
  }

  /**
   * toBuilder.
   *
   * @return a {@link com.google.cloud.bigtable.config.BulkOptions.Builder} object.
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  protected BulkOptions clone() {
    try {
      return (BulkOptions) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Could not clone BulkOptions");
    }
  }
}
