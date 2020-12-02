#### Pipeline Flow
![Pipeline Flow](../../assets/cache_state_externally.png?raw=true)
#### Pipeline Build/Execution Steps

-  To Build the Fat Jar, execute the below command from within the project root directory
```bash
$ (./gradlew clean && ./gradlew shadowJar ;)
```

- To run the pipeline, execute the below command from within the project root directory

```bash
$ (cd build/libs && java -jar CacheState-1.0.jar \
     --region=<GCP region name> \
     --tableInstance=<BigTable Instance Name> \ 
     --tableName=<BigTable Table Name> \
     --columnFamily=<BigTable Column Family> \
     --gcsBucket=<gs://BucketName> \ 
     --fileNamePrefix=<GCS Filename Prefix> \
     --numberOfShards=<Number of Shards for the Sink Files on GCS> \
     --runner=DataflowRunner \
     --jobName=cachestate ;)
```

- To update an existing compatible pipeline, execute the below command from within the project root directory

```bash
$ (cd build/libs && java -jar CacheState-1.0.jar \
     --region=<GCP region name> \
     --tableInstance=<BigTable Instance Name> \ 
     --tableName=<BigTable Table Name> \
     --columnFamily=<BigTable Column Family> \
     --gcsBucket=<gs://BucketName> \ 
     --fileNamePrefix=<GCS Filename Prefix> \
     --numberOfShards=<Number of Shards for the Sink Files on GCS> \
     --runner=DataflowRunner \
     --jobName=cachestate \ 
     --update ;)
```
