#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network.
 Usage: structured_network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Structured Streaming
   would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py
    localhost 9999`
"""
import sys
import os
from time import sleep

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import count

# Set JVM path (OpenJDK 17)os.environ['JAVA_HOME']="/usr/lib/jvm/java-21-openjdk-amd64"
os.environ['JAVA_HOME']="/usr/lib/jvm/java-21-openjdk-amd64"

# packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#     "--packages {0} pyspark-shell".format(packages)
# )

# Load additional packages
conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")

if __name__ == "__main__":
    spark = (SparkSession.builder
        .appName("StructuredNetworkWordCount")
        #.remote("sc://localhost:15002")
        .master("local[*]")
        #.master("spark://192.168.1.84:7077")
        .config(conf = conf)
        .getOrCreate() )

    input_data = (spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "purchases")
                  .option("startingOffsets", "earliest")
                  .load()
                  #.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") )
                  .selectExpr("CAST(value AS STRING)") )
    # SALIDA 1 (append)
    query1 = (input_data.writeStream
        .format("text")
        .outputMode("append")
        .option("path", "salida1.txt")
        .option("checkpointLocation", "chk1")
        .start())

    # SALIDA 2

    query2 = input_data.groupBy("value").count()

    def write_complete(batch_df, batch_id):
        pdf = batch_df.toPandas()

        if not pdf.empty:
            # Sobrescribe el archivo con el estado actualizado
            pdf.to_csv("salida2.txt", mode='w',
                   index=False, header=True)

    stream2 = (query2.writeStream
           .outputMode("complete")
           .foreachBatch(write_complete)
           .start())

    spark.streams.awaitAnyTermination()
