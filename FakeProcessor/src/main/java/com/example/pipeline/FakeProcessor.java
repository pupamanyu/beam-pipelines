package com.example.pipeline;

import com.example.config.FakeProcessorOptions;
import com.example.dofns.FakeDataProducerDoFn;
import com.example.utils.DataPartitioner;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

public class FakeProcessor {
  private static void prepareFakeProcessorPipeline(
      Pipeline p, FakeProcessorOptions fakeProcessorOptions) {
    // Generate Longs as input to Fake Data Generation
    PCollection<Long> longs =
        p.apply(GenerateSequence.from(1).to(fakeProcessorOptions.getNumFakeRows().get()));

    PCollection<TableRow> fakeRows =
        longs.apply("Generate Fake Rows", ParDo.of(new FakeDataProducerDoFn()));

    PCollectionList<TableRow> fakeRowPartitions =
        fakeRows.apply(
            "Partition the fakeRows",
            Partition.of(
                fakeProcessorOptions.getNumPartitions().get(),
                new DataPartitioner(fakeProcessorOptions.getNumPartitions().get())));

    ListIterator<PCollection<TableRow>> fakeRowPartitionsList =
        fakeRowPartitions.getAll().listIterator();

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
            .setProjectId(fakeProcessorOptions.getProject())
            .setDatasetId(fakeProcessorOptions.getDataset().get())
            .setTableId(fakeProcessorOptions.getTable().get());

    while (fakeRowPartitionsList.hasNext()) {
      fakeRowPartitionsList
          .next()
          .apply(
              String.format("Write Fake Partition #%d", fakeRowPartitionsList.nextIndex() - 1),
              BigQueryIO.writeTableRows()
                  .withSchema(tableSchema)
                  .to(tableReference)
                  .optimizedWrites()
                  .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                  .withMethod(BigQueryIO.Write.Method.FILE_LOADS));
    }
  }

  public static void main(String[] args) {
    FakeProcessorOptions fakeProcessorOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FakeProcessorOptions.class);

    Pipeline p = Pipeline.create(fakeProcessorOptions);

    prepareFakeProcessorPipeline(p, fakeProcessorOptions);

    p.run();
  }
}
