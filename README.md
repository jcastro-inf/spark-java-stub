# spark-single-user-rs

This project can be used to perform the experimentation with ALS recommendation.
The proyect can be cloned and compiled with the following commands:
```bash
git clone git@github.com:jcastro-inf/spark-single-user-rs.git
cd spark-single-user-rs
mvn clean package
```

These commands generate a jar file located in target/ named original-spark-single-user-rs-1.0.jar that we will use to submit the job to spark:
```bash
spark-submit \
	--master $SPARK_MASTER \
	--class es.jcastro.delfos.scala.Main \
	target/original-spark-single-user-rs-1.0.jar \
	--input $DATASET \
	--implicitFeedback
```
```
Parameters:
	--master             sets the url of the spark master.
	--class              specifies the entry point of the program.
	--input              specifies the file of the dataset used for the recommendations
	--implicitFeedback   flag to mark the dataset as implicit, thus using implicit als and
                       ignoring rating value in the dataset file, if present.
```

This command needs to be adjusted to match the cluster configuration. Here, there is an example of their values:
```bash
SPARK_MASTER="spark://127.0.0.1:7077"
DATASET="hdfs://hadoop-namenode:8020/datasets/ml-100k/u.data"
```

The algorithm expects the dataset to be a tab separated file in which each line contains an user identifier (integer number), an item identifier. If the file contains more fields, they are ignored. Moreover, the dataset can be provided from local fs or hdfs.
