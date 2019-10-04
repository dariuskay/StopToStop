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
	df = dataframe.groupby('route_id', 'stop_id', 'prev_stop', 'stop_sequence', period) \
		.agg(f.mean('filled_ts').alias('time_per_stop'), \
			f.mean('diff_schedule_real').alias('diff_per_stop'), \
			f.count(f.lit(1)).alias("num_records")) \
		.withColumn("idx", f.monotonically_increasing_id()) \
		.orderBy(['route_id', 'stop_sequence', 'num_records'], ascending=[0, 1, 0]) 
	df = df.filter(df.num_records > 2)
	return df

hour_bin = udf(lambda hour: "01-04" if hour in (1, 2, 3) else ("04-07" if hour in (4, 5, 6) else ("07-10" if hour in (7, 8, 9) else ("10-13" if hour in (10, 11, 12) else ("13-16" if hour in (13, 14, 15) else ("16-19" if hour in (16, 17, 18) else ("19-22" if hour in (19, 20, 21) else "22-01")))))), types.StringType())

def assign_hour(dataframe):
	""" Bin hours into windows. """
	return dataframe.withColumn('hour', hour_bin(dataframe.hour))

def aggregate(dataframe):
	""" Calculates averages based on week, day and hour. """
	return dataframe.groupby('route_id', 'stop_id', 'prev_stop', 'stop_sequence', 'week', 'day', 'hour') \
		.agg(f.mean('filled_ts').alias('time_per_stop'), \
			f.mean('diff_schedule_real').alias('diff_per_stop')) \
		.withColumn('idx', f.monotonically_increasing_id()) \
		.orderBy(['route_id', 'stop_sequence'], ascending=[0,1])
	

def write_to_postgres(dataframe, period, properties):
	""" Creates table in PostgreSQL grouped by PERIOD. No return. """
	dataframe.write.option("createTableColumnTypes", "route_id CHAR(8), stop_id INTEGER, prev_stop INTEGER, stop_sequence SMALLINT, "+period+" SMALLINT, time_per_stop FLOAT, diff_per_stop FLOAT, idx LONG") \
		.option('numParitions', 3) \
		.jdbc(url=config.url, \
               		table=period, mode='overwrite', properties=properties)

def write_total(dataframe, properties):
	dataframe.write.option("createTableColumnTypes", "route_id CHAR(8), stop_id INTEGER, prev_stop INTEGER, stop_sequence SMALLINT, week SMALLINT, day SMALLINT, hour CHAR(8), time_per_stop FLOAT, diff_per_stop FLOAT, idx LONG") \
                .option('numParitions', 3) \
                .jdbc(url=config.url, \
                        table='total', mode='overwrite', properties=properties)


if  __name__ == '__main__':
	scSpark = SparkSession.builder.appName('Stop to Stop: Analytics').getOrCreate()
	scSpark.sparkContext.setLogLevel("ERROR")
	sqlCon = SQLContext(scSpark)	

	preprocessed_file = 's3a://stoptostop/preprocessed/*-preprocessed.csv/*'	
	preprocessed_df = scSpark.read.parquet(preprocessed_file)
	preprocessed_df = preprocessed_df.withColumn('stop_id', preprocessed_df['stop_id'].cast(types.IntegerType()))
	preprocessed_df = preprocessed_df.withColumn('prev_stop', preprocessed_df['prev_stop'].cast(types.IntegerType()))
	preprocessed_df.show()

#	week = average_by(preprocessed_df, 'week')
	binned = assign_hour(preprocessed_df)
	print(binned.head(10))
	total = aggregate(binned)

#	print('WEEK')
#	print(week.head(10))

#	day = average_by(preprocessed_df, 'day')

#	hour = average_by(preprocessed_df, 'hour')
	
	properties = {
            "user": config.postgresUser,
            "password": config.postgresPassword,
			"batchsize": "30000"
        }
	write_total(total, properties)	
#	write_to_postgres(week, 'week', properties)
#	write_to_postgres(day, 'day', properties)
#	write_to_postgres(hour, 'hour', properties)
#	week.write.option("createTableColumnTypes", "route_id CHAR(8), stop_id INTEGER, stop_sequence SMALLINT, week SMALLINT, time_per_stop FLOAT, diff_per_stop FLOAT, idx LONG") \
#		.option('numParitions', 3) \
#			.jdbc(url=config.url, \
#               		table="week", mode='overwrite', properties=properties)
#	day.write.option('createTableColumnTypes', 'route_id CHAR(8), stop_id INTEGER, stop_sequence SMALLINT, day SMALLINT, time_per_stop FLOAT, diff_per_stop FLOAT, idx LONG') \
#			.option('numPartitions', 3) \
#			.jdbc(url=config.url, table='day', mode='overwrite', properties=properties)
#	hour.write.option('createTableColumnTypes', 'route_id CHAR(8), stop_id INTEGER, stop_sequence SMALLINT, hour SMALLINT, time_per_stop FLOAT, diff_per_stop FLOAT, idx LONG') \
#			.option('numPartitions', 3) \
#			.jdbc(url=config.url, table='hour', mode='overwrite', properties=properties)
