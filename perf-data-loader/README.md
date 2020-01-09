# perf-data-loader

#### BigQuery Schema for the tables needed by the pipeline
- Main Table 
```yaml
{
  "fields": [
    {
      "mode": "NULLABLE",
      "name": "TestName",
      "type": "STRING"
    },
    {
      "mode": "NULLABLE",
      "name": "TestURL",
      "type": "STRING"
    },
    {
      "mode": "NULLABLE",
      "name": "TimeStamp",
      "type": "TIMESTAMP"
    },
    {
      "mode": "NULLABLE",
      "name": "NodeName",
      "type": "STRING"
    },
    {
      "mode": "NULLABLE",
      "name": "DNSTime",
      "type": "NUMERIC"
    },
    {
      "mode": "NULLABLE",
      "name": "Connect",
      "type": "NUMERIC"
    },
    {
      "mode": "NULLABLE",
      "name": "SSL",
      "type": "NUMERIC"
    },
    {
      "mode": "NULLABLE",
      "name": "SendTime",
      "type": "NUMERIC"
    },
    {
      "mode": "NULLABLE",
      "name": "WaitTime",
      "type": "NUMERIC"
    },
    {
      "mode": "NULLABLE",
      "name": "Total",
      "type": "NUMERIC"
    }
  ]
}
```

- DeadLetter Table
```yaml
{
  "fields": [
    {
      "mode": "NULLABLE",
      "name": "inputData",
      "type": "STRING"
    },
    {
      "mode": "NULLABLE",
      "name": "errorMessage",
      "type": "STRING"
    }
  ]
}
```

-  To Build the Fat Jar, execute the below command from within the project root directory
```bash
$ ./gradlew clean && ./gradlew shadowJar
```

- To run the pipeline, execute the below command from within the project root directory

```bash
$ cd ../libs && java -jar perf-data-loader-1.0.jar  \
     --dataSet=<Target DataSet>  \
     --table=<Target Table> \
     --deadLetterDataSet=<Dead Letter DataSet> \
     --deadLetterTable=<Dead Letter Table> \
     --runner=DataflowRunner \
     --project=<GCP Project Name>\
     --subscription=projects/<GCP Project Name>/subscriptions/<PubSub Subscription> \
     --jobName=<Pipeline Job Name>
```

- To update an existing compatible pipeline, execute the below command from within the project root directory

```bash
$ cd ../libs && java -jar perf-data-loader-1.0.jar  \
     --dataSet=<Target DataSet>  \
     --table=<Target Table> \
     --deadLetterDataSet=<Dead Letter DataSet> \
     --deadLetterTable=<Dead Letter Table> \
     --runner=DataflowRunner \
     --project=<GCP Project Name>\
     --subscription=projects/<GCP Project Name>/subscriptions/<PubSub Subscription> \
     --jobName=<Existing Pipeline Job Name> \
     --update
```
