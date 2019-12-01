name := "spark-learn"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.7"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.0.5" exclude("com.fasterxml.jackson.core","*")

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-server
libraryDependencies += "org.apache.hbase" % "hbase-server" % "2.0.5" exclude("com.fasterxml.jackson.core","*")

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-common
libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.0.5" exclude("com.fasterxml.jackson.core","*")

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-mapreduce
libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.0.5" exclude("com.fasterxml.jackson.core","*")

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0"

// https://mvnrepository.com/artifact/org.scala-lang/scala-library
libraryDependencies += "org.scala-lang" % "scala-library" % "2.13.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.2"


