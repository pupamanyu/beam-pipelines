/*
 * Copyright (c) 2020 Google Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.TimeZone;

public class CacheStateDoFn extends DoFn<KV<String, Long>, String> {

  private static final Logger LOG = LoggerFactory.getLogger(CacheStateDoFn.class);
  private final String projectID;
  private final String bigTableInstance;
  private final String bigTableName;
  private final String COLUMN_FAMILY_NAME = "cf1";
  private BigtableDataClient bigtableDataClient;
  private ObjectMapper mapper;
  private LoadingCache<String, String> states;

  public CacheStateDoFn(String projectID, String bigTableInstance, String bigTableName) {
    this.projectID = projectID;
    this.bigTableInstance = bigTableInstance;
    this.bigTableName = bigTableName;
  }

  private void updateState(String key, String value) {
    assert bigtableDataClient != null;
    RowMutation rowMutation = RowMutation.create(bigTableName, key)
        .setCell(COLUMN_FAMILY_NAME, ByteString.copyFromUtf8(key),
            System.currentTimeMillis() * 1000, ByteString.copyFromUtf8(value));
    LOG.info("Updated State for {} with {}", key, value);
    bigtableDataClient.mutateRow(rowMutation);
  }

  private String getState(String key) {
    assert bigtableDataClient != null;
    Row row = bigtableDataClient.readRow(bigTableName, key);
    StringBuilder value = new StringBuilder();
    for (RowCell rowCell : row.getCells()) {
      assert rowCell.getValue().toStringUtf8() != null;
      value.append(rowCell.getValue().toStringUtf8());
    }
    return value.toString();
  }

  private void deleteState(String key) {
    RowMutation rowMutation = RowMutation.create(bigTableName, key).deleteRow();
    bigtableDataClient.mutateRow(rowMutation);
  }

  private String stateToJson(State state) {
    String output = null;
    try {
      output = mapper.writeValueAsString(state);
    } catch (JsonProcessingException e) {
      LOG.error(e.toString());
    }
    return output;
  }

  @StartBundle public void startBundle(StartBundleContext startBundleContext) {
    assert states != null;
    assert bigtableDataClient != null;
  }

  @Setup public void doSetup() throws IOException {
    mapper = JsonMapper.builder().build();
    mapper.setTimeZone(TimeZone.getTimeZone("UTC"));
    bigtableDataClient = BigtableDataClient.create(
        BigtableDataSettings.newBuilder().setProjectId(this.projectID)
            .setInstanceId(this.bigTableInstance).build());

    //Cache used BigTable as a backend state
    states = Caffeine.newBuilder().maximumSize(10_000) // Maximum Size of Cache is 10000 rows
        .refreshAfterWrite(Duration.ofMinutes(
            1L)) // Return old value after expiry while cache is getting refreshed after expiry
        .expireAfterWrite(Duration.ofMinutes(60L)) // State expire after 60 minutes
        .writer(new CacheWriter<String, String>() {

          public void write(String key, String value) {
            // Write State
            updateState(key, value);
          }

          public void delete(String key, String value, RemovalCause cause) {
            // Explicit Deletes is handled here
            if (cause == RemovalCause.EXPLICIT) {
              deleteState(key);
            }
          }
        }).build(this::getState);
    assert bigtableDataClient != null;
  }

  @ProcessElement public void processElement(ProcessContext context) {
    /* Update State for the key derived from the input element.
     * Key can be derived fro metadata as well(for eg: Range of keys)
     */
    /* Get the state based on the Key derived from input element.
     * Can be derived using more with metadata as well(for eg: Range of keys)
     */
    RowMutation rowMutation = RowMutation.create(bigTableName, context.element().getKey())
        .setCell(COLUMN_FAMILY_NAME, ByteString.copyFromUtf8(context.element().getKey()),
            System.currentTimeMillis() * 1000,
            ByteString.copyFromUtf8(RandomStringUtils.randomAlphabetic(1)));

    assert bigtableDataClient != null;
    assert states != null;

    // Create/Update State
    State state = State.newBuilder().Key(context.element().getKey())
        .Field1(RandomStringUtils.randomAlphabetic(5)).Field2(RandomStringUtils.randomAlphabetic(5))
        .build();
    // Check if the state for the key exists already
    String cachedValue = states.getIfPresent(context.element().getKey());
    // We can also use .get() and a mapper function to set state atomically instead of getIfPresent() below
    LOG.info("Cached Value for {} is {}", context.element().getKey(),
        states.getIfPresent(context.element().getKey()));
    // Update State if state for the key does not exist
    if (cachedValue == null) {
      states.put(context.element().getKey(), stateToJson(state));
      LOG.info("Cached Value for {} did not exist, and is now set to {}",
          context.element().getKey(), states.getIfPresent(context.element().getKey()));
    }
    context.output(states.getIfPresent(context.element().getKey()));
  }

  @FinishBundle public void finishBundle(FinishBundleContext finishBundleContext) {

  }

  @Teardown public void doTearDown() {
    bigtableDataClient.close();
  }
}
