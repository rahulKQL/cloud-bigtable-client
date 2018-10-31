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
package com.google.cloud.bigtable.grpc.scanner;

import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.common.base.CaseFormat;
import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Parses and runs the acceptance tests for read rows
 */
@RunWith(Parameterized.class)
public class ReadRowsAcceptanceTest {
  // The acceptance test data model, populated via jackson data binding
  private static final class AcceptanceTest {
    public List<ChunkTestCase> tests;
  }

  private static final class ChunkTestCase {
    public String name;
    public List<String> chunks;
    public List<TestResult> results;

    /**
     * The test name in the source file is an arbitrary string. Make it junit-friendly.
     */
    public String getJunitTestName() {
      return CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name.replace(" ", "-"));
    }

    @Override
    public String toString() {
      return getJunitTestName();
    }

    public boolean expectsError() {
      return results != null && !results.isEmpty() && results.get(results.size() - 1).error;
    }

    public List<TestResult> getNonExceptionResults() {
      ArrayList<TestResult> response = new ArrayList<>();
      if (results != null) {
        TestResult previous = null;
        for (TestResult result : results) {
          if (!result.error && !result.equals(previous)) {
            // Dedup the results.  Row merger dedups as well.
            response.add(result);
            previous = result;
          }
        }
      }
      return response;
    }
  }

  private final static class TestResult {
    public String rk;
    public String fm;
    public String qual;
    public long ts;
    public String value;
    public String label;
    public boolean error;

    /**
     * Constructor for JSon deserialization.
     */
    @SuppressWarnings("unused")
    public TestResult() {
    }

    public TestResult(
        String rk, String fm, String qual, long ts, String value, String label, boolean error) {
      this.rk = rk;
      this.fm = fm;
      this.qual = qual;
      this.ts = ts;
      this.value = value;
      this.label = label;
      this.error = error;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("rk", rk)
          .add("fm", fm)
          .add("qual", qual)
          .add("ts", ts)
          .add("value", value)
          .add("label", label)
          .add("error", error)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof TestResult)){
        return false;
      }
      if (obj == this) {
        return true;
      }
      TestResult other = (TestResult) obj;
      return Objects.equals(rk, other.rk)
          && Objects.equals(fm, other.fm)
          && Objects.equals(qual, other.qual)
          && Objects.equals(ts, other.ts)
          && Objects.equals(value, other.value)
          && Objects.equals(label, other.label)
          && Objects.equals(error, other.error);
    }

    @Override
    public int hashCode() {
      return Objects.hash(rk, fm, qual, ts, value, label, error);
    }
  }

  private final ChunkTestCase testCase;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    InputStream testInputStream = ReadRowsAcceptanceTest.class
        .getResourceAsStream("read-rows-acceptance-test.json");

    ObjectMapper mapper = new ObjectMapper();
    try {
      AcceptanceTest acceptanceTest = mapper.readValue(testInputStream, AcceptanceTest.class);
      List<Object[]> data = new ArrayList<>();
      for (ChunkTestCase test : acceptanceTest.tests) {
        data.add(new Object[]{ test });
      }
      return data;
    } catch (IOException e) {
      throw new RuntimeException("Error loading acceptance test file", e);
    }
  }

  public ReadRowsAcceptanceTest(ChunkTestCase testCase) {
    this.testCase = testCase;
  }

  @Test
  public void test() throws Exception {
    // TODO Merge the specified chunks into rows and
    // validate the returned rows against the test results.
    List<FlatRow> responses = new ArrayList<>();
    List<Throwable> exceptions = new ArrayList<>();
    addResponses(responses, exceptions);
    processResults(responses, exceptions);
  }

  private void addResponses(List<FlatRow> responses, List<Throwable> exceptions)
      throws IOException {
    RowMerger rowMerger = createRowMerger(responses, exceptions);

    for (String chunkStr : testCase.chunks) {
      ReadRowsResponse.Builder responseBuilder = ReadRowsResponse.newBuilder();
      CellChunk.Builder ccBuilder = CellChunk.newBuilder();
      TextFormat.merge(new StringReader(chunkStr), ccBuilder);
      responseBuilder.addChunks(ccBuilder.build());
      rowMerger.onNext(responseBuilder.build());
    }
    if (exceptions.isEmpty()) {
      rowMerger.onCompleted();
    }
  }

  private static RowMerger createRowMerger(
      final List<FlatRow> responses, final List<Throwable> exceptions) {
    return new RowMerger(
        new StreamObserver<FlatRow>() {

          @Override
          public void onNext(FlatRow value) {
            responses.add(value);
          }

          @Override
          public void onError(Throwable t) {
            exceptions.add(t);
          }

          @Override
          public void onCompleted() {}
        });
  }

  private void processResults(List<FlatRow> responses, List<Throwable> exceptions) {
    List<TestResult> denormalizedResponses = denormalizeResponses(responses);
    Assert.assertEquals(testCase.getNonExceptionResults(), denormalizedResponses);
    if (testCase.expectsError()) {
      if (exceptions.isEmpty()) {
        Assert.fail("expected exception");
      }
    } else if (!exceptions.isEmpty()) {
      Assert.fail("Got unexpected exception: " + exceptions.get(exceptions.size() - 1).getMessage());
    }
  }

  private List<TestResult> denormalizeResponses(List<FlatRow> responses) {
    ArrayList<TestResult> response = new ArrayList<>();
    for (FlatRow row : responses) {
      for (FlatRow.Cell cell : row.getCells()) {
        response.add(new TestResult(
          toString(row.getRowKey()),
          cell.getFamily(),
          toString(cell.getQualifier()),
          cell.getTimestamp(),
          toString(cell.getValue()),
          cell.getLabels().isEmpty() ? "" : cell.getLabels().get(0),
          false));
      }
    }
    return response;
  }

  protected String toString(final ByteString byteString) {
    return new String(byteString.toByteArray());
  }
}
