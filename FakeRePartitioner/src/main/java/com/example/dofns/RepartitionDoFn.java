package com.example.dofns;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RepartitionDoFn extends DoFn<TableRow, KV<Integer, TableRow>> {
  private final ConcurrentLinkedQueue<Integer> partitions;

  public RepartitionDoFn(Integer numKeys) {
    this.partitions =
        IntStream.range(0, numKeys)
            .boxed()
            .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
  }

  private Integer nextPartition() {
    Integer head = this.partitions.poll();
    this.partitions.add(head);
    return head;
  }

  @ProcessElement
  public void processElement(
      @Element TableRow row, OutputReceiver<KV<Integer, TableRow>> outputReceiver) {
    outputReceiver.output(KV.of(nextPartition(), row));
  }
}
