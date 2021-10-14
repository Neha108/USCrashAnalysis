PySpark script name : US_Crash_Analysis.py
This file contains the solution for US crash anaysis using class and functions which ensures the modularity of the structure.

Spark submit file name : spark_submit.txt

Note: This code has been tested on local system which has PySpark installed along with Jupyter Notebook. Since it was on local system so was not able to run through spark-submit command but we can do it if we have the availability of spark ecosystem.

Note: Since Spark was installed on local system so to read the files in spark dataframe, source data files have been read directly as files were on same path as of code. Using spark in a cluster the files can be read using below sample : 

--if source data file is in local
df = spark.read.format("csv").options(header ="True", inferSchema = "True").load("file:///path_of_file/file_name")

--if source data file is in HDFS
df = spark.read.format("csv").options(header ="True", inferSchema = "True").load("hdfs:///path_of_file/file_name")

Note: As the format of output was not suggested so the output of this analysis will be printed on console. The output can also be written to a file or printed as requirement.