package com.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowToTableRow extends DoFn<Row, TableRow> {
  private static final Logger LOG = LoggerFactory.getLogger(RowToTableRow.class);

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    Row row = processContext.element();
    TableRow tableRow = new TableRow();
    tableRow.set("zipcode", row.getValue("zipcode"));
    tableRow.set("pop_count", row.getValue("pop_count"));
    processContext.output(tableRow);
  }
}

