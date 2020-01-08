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

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;

public class ProcessInsertErrorDoFn extends DoFn<BigQueryInsertError, TableRow> {

  @ProcessElement
  public void processElement(
      @Element BigQueryInsertError element, OutputReceiver<TableRow> receiver) {

    StringBuilder errorMessage = new StringBuilder();
    for (ErrorProto errorProto : element.getError().getErrors()) {
      errorMessage.append(errorProto.toString()).append("\n");
    }
    String inputData = element.getRow().toString();
    TableRow errorRow = new TableRow();
    errorRow.set("inputData", inputData).set("errorMessage", errorMessage);
    receiver.output(errorRow);
  }
}
