package com.example.utils;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.Partition;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataPartitioner implements Partition.PartitionFn<TableRow> {
  private final ConcurrentLinkedQueue<Integer> partitions;

  public DataPartitioner(Integer numPartitions) {
    this.partitions =
        IntStream.range(0, numPartitions)
            .boxed()
            .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
  }

  private Integer nextPartition() {
    Integer head = this.partitions.poll();
    this.partitions.add(head);
    return head;
  }

  @Override
  public int partitionFor(TableRow elem, int partition) {
    return nextPartition();
  }
}
