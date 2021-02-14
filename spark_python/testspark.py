import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("local[*]")\
                    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
rdd = spark.sparkContext.parallelize([ [1, 2, 3], [3, 2, 4], [5, 2, 7] ] )
print('--rdd collect--')
print(rdd.collect())

# function to filter out 2
def filter_out_2(line):
    newLine  = [x for x in line if x != 2]
    return newLine

def filter_out_2_part(line):
    newLine = []
    for subList in line:
        newLine.append([x for x in subList if x != 2])
    return iter(newLine)

def filter_out_2_part_ind(index, line):
    newLine = []
    for subList in line:
        newLine.append((index,[x for x in subList if x != 2]))
    return iter(newLine)


# using map
rddMap = rdd.map(filter_out_2)
print('--rddMap--')
print(rddMap.collect())

# using glom
rddGlom = rdd.glom()
print('--rddGlom--')
print(rddGlom.collect())

# using mapByPartitions
rddMapByPartitions = rdd.mapPartitions(filter_out_2_part)
print('--rddMapByPartitions--')
print(rddMapByPartitions.collect())

# using mapByPartitionswithIndex
rddMapByPartitionsWithIndex = rdd.mapPartitionsWithIndex(filter_out_2_part_ind)
print('--rddMapByPartitionswithindex--')
print(rddMapByPartitionsWithIndex.collect())

print(rddMapByPartitionsWithIndex.sortByKey().collect())

rdd = spark.parallelize([('a',1),('a',2),('b',5),('c',12),('a',15),('b',-8),('d',22)])

rdd_reduceby = rdd.reduceByKey(lambda x,y: x+y )

rdd_groupby = rdd.groupByKey().mapValues(lambda x : sum(x))