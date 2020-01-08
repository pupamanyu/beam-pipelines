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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

public class MetricDoFn extends DoFn<String, TableRow> {
  private static final SimpleModule bigDecimalModule = new SimpleModule();
  private static ObjectMapper mapper;
  private final TupleTag<TableRow> MAIN_OUT;
  private final TupleTag<TableRow> DEADLETTER_OUT;

  MetricDoFn(TupleTag<TableRow> MAIN_OUT, TupleTag<TableRow> DEADLETTER_OUT) {
    this.MAIN_OUT = MAIN_OUT;
    this.DEADLETTER_OUT = DEADLETTER_OUT;
  }

  private static TableRow convertJsonToTableRow(String json) throws IOException {
    try (InputStream inputStream =
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
      return TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
    }
  }

  @Setup
  public void doSetup() {
    bigDecimalModule
        .addSerializer(BigDecimal.class, new CustomBigDecimalSerializer())
        .addDeserializer(BigDecimal.class, new CustomBigDecimalDeserializer());
    mapper =
        JsonMapper.builder()
            .addModule(new ParameterNamesModule())
            .addModule(new Jdk8Module())
            .addModule(new JavaTimeModule())
            .addModule(bigDecimalModule)
            .build();
    mapper.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @ProcessElement
  public void processElement(@Element String element, MultiOutputReceiver receiver) {
    try {
      Metric metric = mapper.readerFor(Metric.class).readValue(element);
      receiver.get(this.MAIN_OUT).output(convertJsonToTableRow(mapper.writeValueAsString(metric)));
    } catch (IOException e) {
      receiver
          .get(this.DEADLETTER_OUT)
          .output(
              new TableRow()
                  .set("inputData", element)
                  .set("errorMessage", ExceptionUtils.getStackTrace(e)));
    }
  }
}
