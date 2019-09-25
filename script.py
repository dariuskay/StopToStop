from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql import types
import pyspark.sql.functions as f
import sys
import copy
# from config import *
import config

def stop_times_to_df(file, sparkSession):
	df = sparkSession.read.csv(file, header=True, sep=',')
	df = df.withColumn('stop_sequence', df['stop_sequence'].cast('int'))
	return df

def vehicle_pos_to_df(file, sparkSession):
	cols_to_drop = ('vehicle_id', 'vehicle_label', 'vehicle_license_plate', \
		'trip_start_time', 'bearing', 'speed', 'stop_status', 'occupancy_status', \
		'congestion_level', 'progress', 'block_assigned', 'dist_along_route', 'dist_from_stop')

	df = sparkSession.read.csv(file, header=True, sep=',').cache()
	df = df.select([column for column in df.columns if column not in cols_to_drop])
	df = df.withColumn('timestamp', df['timestamp'].cast('timestamp'))
	return df

def pos_stop_initial_join(stops_df, pos_df):
	common_cols = ['trip_id', 'stop_id']
	return pos_df.join(stops_df, common_cols) \
		.groupby('trip_id', 'stop_id', 'stop_sequence') \
		.agg(f.min('timestamp').alias('min_timestamp')) \
		.orderBy(*common_cols)

def time_delta(df, window):
	return df['min_timestamp'].cast('long') - f.lag(df['min_timestamp']).over(window)

def df_with_window_time_diff(df, window):
	
	seq_delta = df['stop_sequence'] - f.lag(df['stop_sequence']).over(window)
	time_delta = time_delta(df, window)
	time_per_stop = seq_delta / time_delta

	return df.select(df['trip_id'], \
		df['stop_id'], \
		df['route_id'], \
		df['stop_sequence'], \
		df['min_timestamp'], \
		time_delta.alias('time_delta'), \
		seq_delta.alias('seq_delta'), \
		time_per_stop.alias('time/stop'))

def join_window_df_with_stops_df(windowdf, stopsdf):
	common_cols = ['trip_id', 'stop_id', 'stop_sequence']
	return stopsdf.join(windowdf, common_cols, 'left_outer')

def df_with_window_backfill(df, window1, window2):
	last_stop_id = f.lag(df['stop_id']).over(window1)
	
	df = df.withColumn('prev_stop', last_stop_id)
	
	time_per_stop_backfilled = f.last(df['time/stop'], ignorenulls=True).over(window2))

	df = df.withColumn('filled_ts', time_per_stop_backfilled)

	df = df.withColumn('day', f.dayofweek(df['min_timestamp']))
	df = df.withColumn('hour', f.hour(df['min_timestamp']))

	return df


if  __name__ == '__main__':

        # Creates SparkSession
        scSpark = SparkSession.builder.appName('Stop to Stop').getOrCreate()
	scSpark.sparkContext.setLogLevel("ERROR")
	# And SQLContext
	sqlCon = SQLContext(scSpark)	

        # Any csv's that start with '2019-09-', or all files from September 2019.
        # Each week is approximately 8,906,000 rows.

        stops_file = 'clean_stops.csv'
        data_file = 'miniminiset.csv'
        
        if len(sys.argv) > 1:
                stops_file = sys.argv[1]
                data_file = sys.argv[2]
	
	stops_df = stop_times_to_df(stops_file, scSpark)

        assert stops_df.schema.names is not None
	
	print('STOP TIMES SCHEMA')
	print(stops_df.schema.names)

        # Read csv, put into dataframe. Store in memory.
        updates_df = vehicle_pos_to_df(data_file, scSpark)

        print('UPDATES SCHEMA')
        print(updates_df.schema.names)
        print('='*20)

        print('-'*12)
        print('UPDATES')
        updates_df.show()
	
       	minned_df = stop_pos_initial_join(stops_df, updates_df)
	
	assert len(minned_df.head(1)) != 0

	print('MINNED DF')
	print('='*12)
	print('='*12)
	minned_df.show()
	
	print('Printed schema')
	minned_df.printSchema()
	
	window_look_back_one = Window \
		.partitionBy('trip_id') \
		.orderBy('min_timestamp')
	
	window_df = df_with_window_time_diff(minned_df, window_look_back_one)

	stop_times_joined_df = join_window_df_with_stops_df(window_df, stops_df)
	
	window_look_forward_many = Window \
		.partitionBy('trip_id') \
		.orderBy('min_timestamp') \
		.rowsBetween(0, sys.maxsize)

	# Thanks to John Paton for forward fill (altered here for backward fill).
	# https://johnpaton.net/posts/forward-fill-spark/
	
	df_backfilled = df_with_window_backfill(stop_times_joined_df, window_look_back_one, window_look_forward_many)

	properties = {
            "user": "postgres",
            "password": "postgres"
        }
	
        stop_times_join.write.jdbc(url=config.url, \
                table="data", mode='overwrite', properties=properties)
