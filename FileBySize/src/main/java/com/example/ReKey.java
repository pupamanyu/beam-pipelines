package com.example;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ReKey extends DoFn<String, KV<String, String>> {

  private final String key;

  public ReKey(Options options) {
    this.key = options.getCatalog().get() + options.getTable().get();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    context.output(KV.of(this.key, context.element()));
  }
}
