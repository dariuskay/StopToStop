from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql import types
from pyspark.sql.functions import udf
import pyspark.sql.functions as f
import sys
import copy
import config
import datetime

def average_by(dataframe, period):
	""" Calculates averages by time period PERIOD over DATAFRAME. Returns DataFrame. """
	df = preprocessed_df.groupby('route_id', 'stop_id', 'stop_sequence', period) \
		.agg(f.mean('filled_ts').alias('time_per_stop')) \
		.withColumn("idx", f.monotonically_increasing_id()) \
		.orderBy('route_id', 'stop_sequence', period)
	return df

if  __name__ == '__main__':
	scSpark = SparkSession.builder.appName('Stop to Stop: Analytics').getOrCreate()
	scSpark.sparkContext.setLogLevel("ERROR")
	sqlCon = SQLContext(scSpark)	

	preprocessed_file = 's3a://stoptostop/preprocessed/*-preprocessed.csv/*'	
	preprocessed_df = scSpark.read.parquet(preprocessed_file)
	preprocessed_df = preprocessed_df.withColumn('stop_id', preprocessed_df['stop_id'].cast(types.IntegerType()))


	week = average_by(preprocessed_df, 'week')

	day = average_by(preprocessed_df, 'day')

	hour = average_by(preprocessed_df, 'hour')
	
	properties = {
            "user": config.postgresUser,
            "password": config.postgresPassword,
			"batchsize": "10000"
        }

	week.write.option("createTableColumnTypes", "route_id CHAR(8), stop_id INTEGER, stop_sequence SMALLINT, week SMALLINT, time_per_stop FLOAT, idx LONG") \
		.option('numParitions', 3) \
			.jdbc(url=config.url, \
                		table="week", mode='overwrite', properties=properties)
	day.write.option('createTableColumnTypes', 'route_id CHAR(8), stop_id INTEGER, stop_sequence SMALLINT, day SMALLINT, time_per_stop FLOAT, idx LONG') \
			.option('numPartitions', 3) \
			.jdbc(url=config.url, table='day', mode='overwrite', properties=properties)
	hour.write.option('createTableColumnTypes', 'route_id CHAR(8), stop_id INTEGER, stop_sequence SMALLINT, hour SMALLINT, time_per_stop FLOAT, idx LONG') \
			.option('numPartitions', 3) \
			.jdbc(url=config.url, table='hour', mode='overwrite', properties=properties)