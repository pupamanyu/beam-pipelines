# perf-data-loader
-  To Build the Fat Jar, execute the below command from within the project root directory
```bash
$ ./gradlew clean && ./gradlew shadowJar
```

- To run the pipeline, execute the below command from within the project root directory

```bash
cd ../libs && java -jar perf-data-loader-1.0.jar  \
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
cd ../libs && java -jar perf-data-loader-1.0.jar  \
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
