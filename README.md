## Experimenting spark to cassandra connectivity.
spark_artifact --> Java
spark_python/spark-cassandra.py --> python        

## Starting Cassandra on Mac       
brew install cassandra              
cassandra -f       

### If cassandra startup fails with error Unsafe_GetByte+0xcd, set JAVA_HOME and re-run cassandra        
JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home cassandra -f      


### spark_python/testspark.py is a set of generic useful spark functionalities