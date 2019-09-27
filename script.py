from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql import types
from pyspark.sql.functions import udf
import pyspark.sql.functions as f
import sys
import copy
# from config import *
import config

def stop_times_to_df(file, sparkSession):
	""" Imports `file` to sparkSession, creating and returning a DataFrame"""
	df = sparkSession.read.csv(file, header=True, sep=',')
	df = df.withColumn('stop_sequence', df['stop_sequence'].cast('int'))
	return df

def vehicle_pos_to_df(file, sparkSession):
	""" Imports vehicle position csv and creates a DataFrame. Drops unnecessary columns."""
	cols_to_drop = ('vehicle_id', 'vehicle_label', 'vehicle_license_plate', \
		'trip_start_time', 'bearing', 'speed', 'stop_status', 'occupancy_status', \
		'congestion_level', 'progress', 'block_assigned', 'dist_along_route', 'dist_from_stop')

	df = sparkSession.read.csv(file, header=True, sep=',').cache()
	df = df.select([column for column in df.columns if column not in cols_to_drop])
	df = df.withColumn('timestamp', df['timestamp'].cast('timestamp'))
	return df

def pos_stop_initial_join(stops_df, pos_df):
	""" Joins schedule DataFrame with positions DataFrame. Returns a DataFrame
	with only the minimum timestamp per stop in stop sequence."""
	common_cols = ['trip_id', 'stop_id']
	return pos_df.join(stops_df, common_cols) \
		.groupby('trip_id', 'stop_id', 'stop_sequence') \
		.agg(f.min('timestamp').alias('min_timestamp'), \
		f.first('route_id').alias('route_id')) \
		.orderBy(*common_cols)

def time_delta(df, window):
	""" Performs subtraction over timestamps, returning the difference between a timestamp
	and the latest timestamp for that trip_id. Returns difference in seconds."""
	return df['min_timestamp'].cast('long') - f.lag(df['min_timestamp']).over(window).cast('long')

def df_with_window_time_diff(df, window):
	""" Performs subtraction over latest stop sequence and current stop_sequence in order
	to calculate time per stop between timestamps. Adds result as column in DataFrame. Returns DataFrame."""
	seq_delta = df['stop_sequence'] - f.lag(df['stop_sequence']).over(window)
	time = time_delta(df, window)
	time_per_stop = time / seq_delta

	return df.select(df['trip_id'], \
		df['stop_id'], \
		df['route_id'], \
		df['stop_sequence'], \
		df['min_timestamp'], \
		time.alias('time_delta'), \
		seq_delta.alias('seq_delta'), \
		time_per_stop.alias('time/stop'))

def join_window_df_with_stops_df(windowdf, stopsdf):
	""" Performs a left join on windowed DataFrame with schedule DataFrame."""
	common_cols = ['trip_id', 'stop_id', 'stop_sequence']
	return stopsdf.join(windowdf, common_cols, 'left_outer')

def join_window_df_with_stops_df_sql(table1, table2, sqlContext):
	""" In lieu of the non-SQL version working, filters schedule for trip_ids in 
	windowed stops table (significantly reducing table size). That result is joined
	with windowed stops table, returning DataFrame."""
	to_join =  sqlContext.sql('SELECT * FROM '+table1+' WHERE '+table1+'.trip_id IN (SELECT DISTINCT trip_id FROM '+table2+')')
	print('FROM WITHIN join_window_df_with_stops_df_sql')
	print('SUBQUERIED')
	to_join.show()
	to_join.createOrReplaceTempView('subqueried')
	
	return sqlContext.sql('SELECT subqueried.*, '+table2+'.route_id, '+table2+'.min_timestamp, '+table2+'.time_delta, '+table2+'.seq_delta, '+table2+'.`time/stop` FROM subqueried LEFT JOIN '+table2+' ON subqueried.trip_id='+table2+'.trip_id AND subqueried.stop_id='+table2+'.stop_id AND subqueried.stop_sequence='+table2+'.stop_sequence ORDER BY trip_id, stop_sequence')	


def df_with_window_backfill(df, window1, window2):
	""" Provides new rows (generated from schedule) so that for each trip_id, intermediate
	stop_ids are included. For each of these new stop_ids, backfill via window time/stop. Maps week of year,
	day of week, week of year.
	Returns a DataFrame where no time/stop value is less than 0 (for the case in which a trip erroneously
	'starts over.'"""
	last_stop_id = f.lag(df['stop_id']).over(window1)
	
	df = df.withColumn('prev_stop', last_stop_id)
	df = df.withColumn('filled_ts', f.first(df['time/stop'], ignorenulls=True).over(window2))
	df = df.withColumn('day', f.first(f.dayofweek(df['min_timestamp']), ignorenulls=True).over(window2))
	df = df.withColumn('hour', f.first(f.hour(df['min_timestamp']), ignorenulls=True).over(window2))
	
	columns_to_return = ['trip_id', 'stop_id', 'prev_stop', 'stop_sequence', 'route_id', 'filled_ts', 'day', 'hour']
	df = df.filter(df.filled_ts.isNotNull()) \
		.filter((df.filled_ts > 0)) \
		.select(*columns_to_return)

	return df

replace_dummy = udf(lambda timefill: None if timefill == 6666.6666 else timefill, types.DoubleType())

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
	
       	minned_df = pos_stop_initial_join(stops_df, updates_df)
	
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

	print('WINDOW')
	window_df.show()

	window_df_filled = window_df.fillna(6666.6666, subset=['time/stop'])
	
	print('WINDOW FILLEDNA')
	window_df_filled.show()

	window_df_filled.createOrReplaceTempView('window_filled')
	stops_df.createOrReplaceTempView('stops')

	# stop_times_joined_df = join_window_df_with_stops_df(window_df, stops_df)
	stop_times_joined_df = join_window_df_with_stops_df_sql('stops', 'window_filled', sqlCon)	
	
	print('REDUCED')
	stop_times_joined_df.show()

	window_look_forward_many = Window \
		.partitionBy('trip_id') \
		.orderBy('trip_id', 'stop_sequence') \
		.rowsBetween(0, sys.maxsize)

	# Thanks to John Paton for forward fill (altered here for backward fill).
	# https://johnpaton.net/posts/forward-fill-spark/
	
	df_backfilled = df_with_window_backfill(stop_times_joined_df, window_look_back_one, window_look_forward_many)

	print('BACKFILLED, DIRTY')
	df_backfilled.show()
	
#	df_backfilled = df_backfilled.filter((df_backfilled.route_id.isNotNull()) & (df_backfilled.filled_ts != 6666.6666
#	df_backfilled = df_backfilled.filter('route_id IS NOT NULL AND filled_ts != 6666.6666')
	df_backfilled = df_backfilled.filter('filled_ts != 6666.6666')
	
	print('BACKFILLED, FILTERED')
	df_backfilled.show()

#	df_backfilled = df_backfilled.withColumn('filled_ts', replace_dummy(df_backfilled.filled_ts))	
	
#	print('BACKFILLED, CLEANED')
#	df_backfilled.show()	
	
	print('Writing to database...')
	properties = {
            "user": "postgres",
            "password": "postgres"
        }
	
        df_backfilled.write.jdbc(url=config.url, \
                table="data", mode='overwrite', properties=properties)
