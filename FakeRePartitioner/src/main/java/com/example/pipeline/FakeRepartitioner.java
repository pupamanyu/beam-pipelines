package com.example.pipeline;

import com.example.config.FakeRepartitionOptions;
import com.example.dofns.FakeDataProducerDoFn;
import com.example.dofns.PrepareTableRowDoFn;
import com.example.dofns.RepartitionDoFn;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class FakeRepartitioner {
  private static void prepareFakeRepartitionPipeline(
      Pipeline p, FakeRepartitionOptions fakeRePartitionOptions) {
    // Generate Longs as input to Fake Data Generation
    PCollection<Long> longs =
        p.apply(GenerateSequence.from(1).to(fakeRePartitionOptions.getNumFakeRows().get()));

    PCollection<TableRow> fakeRows =
        longs.apply("Generate Fake Rows", ParDo.of(new FakeDataProducerDoFn()));

    PCollection<KV<Integer, TableRow>> fakeRowPartitions =
        fakeRows.apply(
            "Add Keys to the fakeRows",
            ParDo.of(new RepartitionDoFn(fakeRePartitionOptions.getNumPartitions().get())));

    List<TableFieldSchema> tableFieldSchema =
        Arrays.asList(
            new TableFieldSchema().setName("email").setMode("NULLABLE").setType("STRING"),
            new TableFieldSchema().setName("uid").setMode("NULLABLE").setType("STRING"),
            new TableFieldSchema().setName("streetName").setMode("NULLABLE").setType("STRING"),
            new TableFieldSchema().setName("buildingNumber").setMode("NULLABLE").setType("STRING"),
            new TableFieldSchema().setName("city").setMode("NULLABLE").setType("STRING"),
            new TableFieldSchema().setName("country").setMode("NULLABLE").setType("STRING"),
            new TableFieldSchema().setName("firstName").setMode("NULLABLE").setType("STRING"),
            new TableFieldSchema().setName("lastName").setMode("NULLABLE").setType("STRING"));

    TableSchema tableSchema = new TableSchema().setFields(tableFieldSchema);

    TableReference tableReference =
        new TableReference()
            .setProjectId(fakeRePartitionOptions.getProject())
            .setDatasetId(fakeRePartitionOptions.getDataset().get())
            .setTableId(fakeRePartitionOptions.getTable().get());

    fakeRowPartitions
        .apply(GroupByKey.create())
        .apply("Prepare TableRow", ParDo.of(new PrepareTableRowDoFn()))
        .apply(
            BigQueryIO.writeTableRows()
                .withSchema(tableSchema)
                .to(tableReference)
                .optimizedWrites()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API));
  }

  public static void main(String[] args) {
    FakeRepartitionOptions fakeRePartitionOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FakeRepartitionOptions.class);

    Pipeline p = Pipeline.create(fakeRePartitionOptions);

    prepareFakeRepartitionPipeline(p, fakeRePartitionOptions);

    p.run();
  }
}
