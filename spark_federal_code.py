import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame , Row, functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import datetime
from datetime import datetime, timedelta
import math
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

master = "local"
appName = "test1"
driver_path = "/home/spark-out/spark-jars/"
source_conn_details=ast.literal_eval(os.environ.get('source_conn_info'))
target_conn_details=ast.literal_eval(os.environ.get('target_conn_info'))
source_query = os.environ.get('source_query')
target_query = os.environ.get('target_query')

print("--------------------source_conn_details--------------------",source_conn_details)
print("target_conn_details------------------------",target_conn_details)
print("----------------------source_query-----------",source_query)
print("----------------target_query-------------------",target_query)


def init():
    driver_list = ['ojdbc8.jar', 'ojdbc8.jar']
    driver_list = list(set(driver_list))
    print(driver_list)
    full_driver_path=[os.path.join(driver_path,each) for each in driver_list]
    print(full_driver_path)
    if len(full_driver_path)>0:
        #spark = SparkSession.builder.appName(appName).master(master).config("spark.driver.extraClassPath",":".join(full_driver_path)).getOrCreate()
		spark = SparkSession.builder.appName(appName).master(master).config("spark.driver.extraClassPath",":".join(full_driver_path)).config("spark.sql.decimalOperations.allowPrecisionLoss", "true").getOrCreate()

    else:
        spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def func_code_execution(spark):
    start_time = datetime.now()
    source_url = f"jdbc:oracle:thin:@//{source_conn_details['host_address']}:{source_conn_details['port_number']}/{source_conn_details['database_name']}"
    source_properties ={"user":source_conn_details["username"], "password" :source_conn_details["password"],"driver":"oracle.jdbc.driver.OracleDriver"}
    print("----source_properties-------",source_properties)
    source_df = spark.read.format("jdbc").options(url=source_url, dbtable=source_query, **source_properties).option("fetchsize",1000000).option('schema', schema2).load()

    target_url = f"jdbc:oracle:thin:@//{target_conn_details['host_address']}:{target_conn_details['port_number']}/{target_conn_details['database_name']}"
    target_properties ={"user":target_conn_details["username"], "password" : target_conn_details["password"],"driver":"oracle.jdbc.driver.OracleDriver"}
    source_df.write.format("jdbc").mode("overwrite").options(url=target_url, dbtable=target_query, **target_properties).option("batchsize",100000).option("numPartitions",9).option('schema', schema2).option("jdbcParam", {"DECIMAL_TYPE": "NUMBER(38, 40)"}).save()
    print("Total Data Migration Successfully")
    end_time = datetime.now()
    time_taken = end_time - start_time
    print("time_taken=" + str(time_taken))
    spark.stop()


if __name__ == '__main__':
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start -> Start_time: {} ".format(current_time))
    spark = init()
    func_code_execution(spark)
    print("End -> End_time: {} ".format(current_time))
	
--------------------------------------------------------------------------------------------------------------------------------------------------------------
import os
import sys
import time
import math
import ast
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame , Row, functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

master = "local"
appName = "test06022024"
driver_path = "/home/spark-out/spark-jars/"
source_conn_details=ast.literal_eval(os.environ.get('source_conn_info'))
target_conn_details=ast.literal_eval(os.environ.get('target_conn_info'))
source_query = os.environ.get('source_query')
target_query = os.environ.get('target_query')
source_query= """(SELECT OBJ_PACK, to_char(CRC32) AS CRC32 , FBO_ID_TYPE, FBO_ID_NUM, FBO_ID_VER, ALL_STATUS, ACTION, USER_ID, TRANTIME, ENTITY_FBO_ID_NUM, SEC_DEFN_FBO_ID_NUM, EOD_DATE, to_char(BID_PRICE) AS BID_PRICE, to_char(OFFER_PRICE) AS OFFER_PRICE, to_char(BID_YIELD) AS BID_YIELD, to_char(OFFER_YIELD) AS OFFER_YIELD, to_char(TRADED_SPREAD) AS TRADED_SPREAD, LAST_TRADED_DATE, OLD_ALL_STATUS FROM ftprod.TEST_PRICES_ARCHIVE_SOURCE) temp"""



print("--------------------source_conn_details--------------------",source_conn_details)
print("target_conn_details------------------------",target_conn_details)
print("----------------------source_query-----------",source_query)
print("----------------target_query-------------------",target_query)

schema1 = StructType([
    StructField("OBJ_PACK",StringType(),True),
    StructField("CRC32",StringType(),True),
    StructField("FBO_ID_TYPE",StringType(),False),
    StructField("FBO_ID_NUM",StringType(),False),
    StructField("FBO_ID_VER",StringType(),False),
    StructField("ALL_STATUS",StringType(),True),
    StructField("ACTION",StringType(),False),
    StructField("USER_ID",StringType(),False),
    StructField("TRANTIME",DateType(),False),
    StructField("ENTITY_FBO_ID_NUM",StringType(),False),
    StructField("SEC_DEFN_FBO_ID_NUM",StringType(),False),
    StructField("EOD_DATE",DateType(),False),
    StructField("BID_PRICE",StringType(),True),
    StructField("OFFER_PRICE",StringType(),True),
    StructField("BID_YIELD",StringType(),True),
    StructField("OFFER_YIELD",StringType(),True),
    StructField("TRADED_SPREAD",StringType(),True),
    StructField("LAST_TRADED_DATE",DateType(),True),
    StructField("OLD_ALL_STATUS",StringType(),True)
])



schema2 = StructType([
    StructField("OBJ_PACK",StringType(),True),
    StructField("CRC32",IntegerType(),True),
    StructField("FBO_ID_TYPE",StringType(),False),
    StructField("FBO_ID_NUM",IntegerType(),False),
    StructField("FBO_ID_VER",IntegerType(),False),
    StructField("ALL_STATUS",StringType(),True),
    StructField("ACTION",StringType(),False),
    StructField("USER_ID",StringType(),False),
    StructField("TRANTIME",DateType(),False),
    StructField("ENTITY_FBO_ID_NUM",IntegerType(),False),
    StructField("SEC_DEFN_FBO_ID_NUM",IntegerType(),False),
    StructField("EOD_DATE",DateType(),False),
    StructField("BID_PRICE",IntegerType(),True),
    StructField("OFFER_PRICE",IntegerType(),True),
    StructField("BID_YIELD",IntegerType(),True),
    StructField("OFFER_YIELD",IntegerType(),True),
    StructField("TRADED_SPREAD",IntegerType(),True),
    StructField("LAST_TRADED_DATE",DateType(),True),
    StructField("OLD_ALL_STATUS",StringType(),True)
])

def init():
    driver_list = ['ojdbc8.jar', 'ojdbc8.jar']
    driver_list = list(set(driver_list))
    print(driver_list)
    full_driver_path=[os.path.join(driver_path,each) for each in driver_list]
    print(full_driver_path)
    if len(full_driver_path)>0:
        spark = SparkSession.builder.appName(appName).master(master).config("spark.driver.extraClassPath",":".join(full_driver_path)).getOrCreate()
    else:
        spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def func_code_execution(spark):
    start_time = datetime.now()
    source_url = f"jdbc:oracle:thin:@{source_conn_details['host_address']}:{source_conn_details['port_number']}/{source_conn_details['database_name']}"
    source_properties ={"user":source_conn_details["username"], "password" :source_conn_details["password"],"driver":"oracle.jdbc.driver.OracleDriver"}
    source_df = spark.read.format("jdbc").options(url=source_url, dbtable=source_query, **source_properties).option("fetchsize",1000000).load()
#    for c_name in source_df.columns:
#        if isinstance(source_df.schema[c_name].dataType,DecimalType):
#            source_df = source_df.withColumn(c_name, col(c_name).cast(DecimalType(38,38)))
    source_df.show(10)
    target_url = f"jdbc:oracle:thin:@{target_conn_details['host_address']}:{target_conn_details['port_number']}/{target_conn_details['database_name']}"
    target_properties ={"user":target_conn_details["username"], "password" : target_conn_details["password"],"driver":"oracle.jdbc.driver.OracleDriver"}
    source_df.write.format("jdbc").mode("append").options(url=target_url, dbtable=target_query, **target_properties).option("batchsize",100000).option("numPartitions",9).save()
    source_df.printSchema()
    print("Total Data Migration Successfully")
    end_time = datetime.now()
    time_taken = end_time - start_time
    print("time_taken=" + str(time_taken))
    spark.stop()


if __name__ == '__main__':
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start -> Start_time: {} ".format(current_time))
    spark = init()
    func_code_execution(spark)
    print("End -> End_time: {} ".format(current_time))


