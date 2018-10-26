package com.example;

import java.util.Arrays;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONObject;
import org.json.JSONException;

import java.util.List;
import java.util.stream.Collectors;

public class ExtractCustomDoFn extends DoFn<String, String> {

  public static final TupleTag<String> EXTRACTCUSTOM_SUCCESS = new TupleTag<String>() {
  };
  public static final TupleTag<KV<String, String>> EXTRACTCUSTOM_FAILED = new TupleTag<KV<String, String>>() {
  };
  private final Boolean exclude;
  private final List<String> validCustomTypes;
  private final String customDataTypeFieldSelector;
  private final String customDataTypeExcludingFieldSelectorValue;
  private final List<String> filter;
  private final String customDataType;
  private final Counter extractedCustomRows;
  private final Counter extractedExcludedCustomRows;
  private final Counter discardedCustomRows;
  private final Counter failedExtractCustomRows;

  public ExtractCustomDoFn(Boolean exclude, String customDataType, String customFields,
          String validCustomTypes, String customDataTypeFieldSelector,
          String customDataTypeExcludingFieldSelectorValue) {
    this.exclude = exclude;
    this.customDataType = customDataType;
    this.validCustomTypes = Arrays.asList(validCustomTypes.split(",")).stream().map(String::trim).collect(Collectors.toList());
    this.customDataTypeFieldSelector = customDataTypeFieldSelector;
    this.customDataTypeExcludingFieldSelectorValue = customDataTypeExcludingFieldSelectorValue;
    if (this.validCustomTypes.stream().noneMatch(this.customDataType::equals)) {
      throw new RuntimeException(
              String.format("Initialization Error. Invalid Custom Data Type %s provided. Please check", this.customDataType));
    }
    this.filter = Arrays.asList(customFields.split(",")).stream().map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    this.extractedCustomRows = Metrics.counter(ExtractCustomDoFn.class, this.customDataType + "-row-counts");
    this.extractedExcludedCustomRows = Metrics.counter(ExtractCustomDoFn.class, this.customDataType + "-excluded-row-counts");
    this.discardedCustomRows = Metrics.counter(ExtractCustomDoFn.class, "filteredout-row-counts");
    this.failedExtractCustomRows = Metrics.counter(ExtractCustomDoFn.class, "failed-customextract-row-counts");
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    try {
      JSONObject element = new JSONObject(context.element());
      if (element.has(customDataTypeFieldSelector) && element.get(customDataTypeFieldSelector) instanceof String) {
        if (exclude && this.customDataType.equals(customDataTypeExcludingFieldSelectorValue)
                && this.filter
                        .stream()
                        .map(String::toLowerCase)
                        .noneMatch(element.getString(customDataTypeFieldSelector).toLowerCase()::equals)) {
          context.output(EXTRACTCUSTOM_SUCCESS, context.element());
          this.extractedExcludedCustomRows.inc();
        } else if (!exclude && this.filter
                .stream().map(String::toLowerCase)
                .anyMatch(element.getString(customDataTypeFieldSelector).toLowerCase()::matches)) {
          context.output(EXTRACTCUSTOM_SUCCESS, context.element());
          this.extractedCustomRows.inc();
        } else {
          this.discardedCustomRows.inc();
        }
      } else {
        KV<String, String> errorData = KV.of(
                context.element(),
                String.format("{\"message\":\"selector field not usable: %s\"}", customDataTypeFieldSelector));
        context.output(EXTRACTCUSTOM_FAILED, errorData);
        this.failedExtractCustomRows.inc();
      }
    } catch (JSONException jsex) {
      KV<String, String> errorData = KV.of(
              context.element(),
              String.format("{\"message\":\"%s\"}", jsex.getMessage()));
      context.output(EXTRACTCUSTOM_FAILED, errorData);
      this.failedExtractCustomRows.inc();
    }
  }
}
