#### Pipeline Build/Execution Steps

-  To Build the Fat Jar, execute the below command from within the project root directory
```bash
$ (./gradlew clean && ./gradlew shadowJar ;)
```

- To run the pipeline, execute the below command from within the project root directory

```bash
$ (cd build/libs && java -jar CacheState-1.0.jar \
     --region=<GCP region name> \
     --bigTableInstance=<bigtable instance name> \ 
     --bigTableName=<bigtable table name> \
     --gcsBucket=<gs://bucket> \ 
     --fileNamePrefix=<gcs filename prefix> \
     --numberOfShards=<number of shards for the sink files on GCS> \
     --runner=DataflowRunner \
     --jobName=cachestate ;)
```

- To update an existing compatible pipeline, execute the below command from within the project root directory

```bash
$ (cd build/libs && java -jar CacheState-1.0.jar \
     --region=<GCP region name> \
     --bigTableInstance=<bigtable instance name> \ 
     --bigTableName=<bigtable table name> \
     --gcsBucket=<gs://bucket> \ 
     --fileNamePrefix=<gcs filename prefix> \
     --numberOfShards=<number of shards for the sink files on GCS> \
     --runner=DataflowRunner \
     --jobName=cachestate \ 
     --update ;)
```
