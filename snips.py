
# coding: utf-8

# In[ ]:

df <- loadDF("F:/file.csv", "csv", header = "true")


# In[ ]:

##### CELL C #####

#import hadoop
def set_hadoop_config():
    
    # Turns out setting parameters via hadoopConfiguration is (apparently) not useful for spark 2.0
    
    #Required parameter names listed in http://spark.apache.org/docs/latest/storage-openstack-swift.html

    prefix = "fs.hdfs.service." + appName  #some refs use creds['name'], which can be any string
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set(prefix + ".auth.url", r'http://172.18.0.2' )
    hconf.set(prefix + ".username", 'root')
    hconf.set(prefix + ".http.port", '9000')
    hconf.setBoolean(prefix + ".public", True)

    # TRY: download jars & add to Dockerfile
    #org.apache.hadoop.
    #hconf.set("fs.hdfs.impl", hdfs.DistributedFileSystem.getName());
    #hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.getName());
    #hconf.set("fs.swift.impl", "org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem")


# In[ ]:

#this is stuff I might yet use

from __future__ import print_function
import sys
from pyspark.sql import SparkSession


# In[ ]:

#figuring out dates
from datetime import datetime
 #pyspark.sql.types.DateType

    def parse_clf_time(s):
    """ Convert Common Log time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring time zone here. In a production application, you'd want to handle that.
    return "{0:02d}-{1:02d}-{2:04d} {3:02d}:{4:02d}:{5:02d}".format(
      int(s[7:11]),
      month_map[s[3:6]],
      int(s[0:2]),
      int(s[12:14]),
      int(s[15:17]),
      int(s[18:20])
    )

def to_date_obj(str,format):
    return datetime.strptime(str, format)

u_date_conv = spark.udf.register('to_date_obj')  

data_date_format=r'%m/%d/%y'

