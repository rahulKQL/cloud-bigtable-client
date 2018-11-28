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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.cloud.bigtable.data.v2.models.Query;
import org.apache.hadoop.hbase.client.Operation;

/**
 * Interface used for Scan and Get operation adapters.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface ReadOperationAdapter<T extends Operation> {
  /**
   * <p>adapt.</p>
   *
   * @param request a T object.
   * @param readHooks a {@link ReadHooks} object.
   * @param query a  {@link Query} object.
   */
  void adapt(T request, ReadHooks readHooks, Query query);
}
