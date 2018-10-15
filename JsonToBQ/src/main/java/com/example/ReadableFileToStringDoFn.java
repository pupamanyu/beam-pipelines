/*
#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
*/

package com.example;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

class ReadableFileToStringDoFn extends DoFn<FileIO.ReadableFile, String> {

  public static final TupleTag<String> VALIDROWS = new TupleTag<String>() {};
  public static final TupleTag<ReadableFile> FAILEDFILES = new TupleTag<ReadableFile>() {};
  private final Counter validRows;
  private final Counter failedFiles;

  public ReadableFileToStringDoFn() {
    this.validRows = Metrics.counter(ReadableFileToStringDoFn.class, "valid-rows");
    this.failedFiles = Metrics.counter(ReadableFileToStringDoFn.class, "invalid-files");
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    try {
      try (InputStream inputStream = Channels.newInputStream(context.element().open());
          BufferedReader bufferedReader =
              new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          this.validRows.inc();
          context.output(VALIDROWS, line);
        }
      }
    } catch (IOException e) {
      this.failedFiles.inc();
      context.output(FAILEDFILES, context.element());
    }
  }
}
