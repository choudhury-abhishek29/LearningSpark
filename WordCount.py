import os
import sys

def f(x): print x
#Should be a better way to do this
# Path for spark source folder
os.environ['SPARK_HOME']="E:/setups/spark-2.0.0-bin-hadoop2.7"
# Append pyspark  to Python Path
sys.path.append("E:/setups/spark-2.0.0-bin-hadoop2.7/bin/pyspark")
#
try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    sc = SparkContext('local')
    print ("Successfully imported Spark Modules")

    text_file = sc.textFile("E:/a.txt")
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    print counts.collect()
    # print counts.foreach(f)

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)