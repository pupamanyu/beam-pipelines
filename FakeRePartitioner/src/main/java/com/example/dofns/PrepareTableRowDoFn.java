package com.example.dofns;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class PrepareTableRowDoFn extends DoFn<KV<Integer, Iterable<TableRow>>, TableRow> {
    @ProcessElement
    public void processElement(@Element KV<Integer, Iterable<TableRow>> iterableKV, OutputReceiver<TableRow> outputReceiver) {
        for (TableRow row : iterableKV.getValue()) {
            outputReceiver.output(row);
        }
    }
}
