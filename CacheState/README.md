#### Pipeline Flow
<a href="url"><img src="../../assets/cache_state_externally.png" alt="Diagram" width="250x" ></a>

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



[![GitHub issues](https://img.shields.io/github/issues/pupamanyu/beam-pipelines?style=plastic)](https://github.com/pupamanyu/beam-pipelines/issues)
[![GitHub forks](https://img.shields.io/github/forks/pupamanyu/beam-pipelines?style=plastic)](https://github.com/pupamanyu/beam-pipelines/network)
[![GitHub stars](https://img.shields.io/github/stars/pupamanyu/beam-pipelines?style=plastic)](https://github.com/pupamanyu/beam-pipelines/stargazers)
[![GitHub license](https://img.shields.io/github/license/pupamanyu/beam-pipelines?style=plastic)](https://github.com/pupamanyu/beam-pipelines/blob/master/LICENSE)
