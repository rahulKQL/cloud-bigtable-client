/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters.filters;

import java.io.IOException;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.hbase.filter.BigtableFilter;

/**
 * Converts a {@link BigtableFilter} to a {@link RowFilter}.
 */
public class BigtableFilterAdapter extends TypedFilterAdapterBase<BigtableFilter> {

  @Override
  public Filter adapt(FilterAdapterContext context, BigtableFilter filter) throws IOException {
    return filter.getFilter();
  }

  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context,
      BigtableFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
