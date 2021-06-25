To generate the dataset, run the following commands:
```shell
# This will extract all of the locations of *.gov.ro
# web pages from Common Crawl
./ccgrep-cdx '*.gov.ro' CC-MAIN-2021-25 >gov.ro.jl

# This will fetch them from S3 and write them to a
# compressed warc
cat gov.ro.jl | ./ccget.py | gzip -9 >gov.ro.warc.gz

# This will generate the final dataset which consists
# of the anotated vertices and edges of the graph
java -cp ../build/libs/salsa-bench-all.jar \
  '-Dspark.master=local[*]' SalsaBench.App \
  subgraph gov.ro.warc.gz
```
