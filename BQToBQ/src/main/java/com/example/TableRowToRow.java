package com.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRowToRow extends DoFn<TableRow, Row> {
  private static final Schema schema =
      Schema.builder().addStringField("zipcode").addInt64Field("population").build();
  private static final Logger LOG = LoggerFactory.getLogger(RowToTableRow.class);

  @ProcessElement
  public void ProcessElement(ProcessContext pc) {
    try {
      TableRow tableRow = pc.element();

      String zipCode = (String) tableRow.getOrDefault("zipcode", null);
      String populationStr = (String) tableRow.getOrDefault("population", "0");
      Long population = Long.parseLong(populationStr);
      Row row = Row.withSchema(schema).addValues(zipCode, population).build();
      pc.output(row);
    } catch (Exception e) {
      LOG.warn("Exception occurred in Convert TableRow to Row: " + e);
    }
  }
}

