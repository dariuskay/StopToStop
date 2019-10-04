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

def stop_times_to_df(file, sparkSession, trips_df):
	""" 
	Imports `file` to sparkSession, creating and returning a DataFrame joined
	with trips-routes relationship.
	"""
	df = sparkSession.read.csv(file, header=True, sep=',')
	df = df.join(trips_df, ['trip_id'])
	df = df.withColumn('stop_sequence', df['stop_sequence'].cast('int'))
	df = df.withColumn('stop_time', df['stop_time'].cast('timestamp'))

	return df

def add_schedule_difference(df, window):
	"""
	Calculates difference between row and previous schedule times. Add as column.
	"""
	time_diff = df['stop_time'].cast('long') - f.lag(df['stop_time']).over(window).cast('long')

	df = df.withColumn('schedule_delta', time_diff) 

	return df

def vehicle_pos_to_df(file, sparkSession):
	""" 
	Imports vehicle position csv and creates a DataFrame. Drops unnecessary columns.
	"""
	cols_to_drop = ('vehicle_id', 'vehicle_label', 'vehicle_license_plate', \
		'trip_start_time', 'bearing', 'speed', 'stop_status', 'occupancy_status', \
		'congestion_level', 'progress', 'block_assigned', 'dist_along_route', 'dist_from_stop')

	df = sparkSession.read.csv(file, header=True, sep=',').cache()
	df = df.select([column for column in df.columns if column not in cols_to_drop])
	df = df.withColumn('timestamp', df['timestamp'].cast('timestamp'))
	return df

def pos_stop_initial_join(stops_df, pos_df):
	""" 
	Joins schedule DataFrame with positions DataFrame. Returns a DataFrame
	with only the minimum timestamp per stop in stop sequence.
	"""
	common_cols = ['trip_id', 'stop_id', 'route_id']
	return pos_df.join(stops_df, common_cols) \
		.groupby('route_id', 'trip_id', 'stop_id', 'stop_sequence') \
		.agg(f.min('timestamp').alias('min_timestamp')) \
		.orderBy('trip_id', 'stop_id')

def time_delta(df, window):
	""" 
	Performs subtraction over timestamps, returning the difference between a timestamp
	and the latest timestamp for that trip_id. Returns difference in seconds.
	"""
	return df['min_timestamp'].cast('long') - f.lag(df['min_timestamp']).over(window).cast('long')

def df_with_window_time_diff(df, window):
	""" 
	Performs subtraction over latest stop sequence and current stop_sequence in order
	to calculate time per stop between timestamps. Adds result as column in DataFrame. Returns DataFrame.
	"""
	seq_delta = df['stop_sequence'] - f.lag(df['stop_sequence']).over(window)
	time = time_delta(df, window)
	time_per_stop = time / seq_delta

	return df.select(df['trip_id'], \
		df['stop_id'], \
		df['route_id'], \
		df['stop_sequence'], \
		df['min_timestamp'], \
#		df['schedule_delta'], \
		time.alias('time_delta'), \
		seq_delta.alias('seq_delta'), \
		time_per_stop.alias('time/stop')) \
		.fillna(6666.6666, subset=['time/stop'])

def join_window_df_with_stops_df(windowdf, stopsdf):
	""" 
	Performs a left join on windowed DataFrame with schedule DataFrame.
	"""
	common_cols = ['trip_id', 'stop_id', 'stop_sequence']
	return stopsdf.join(windowdf, common_cols, 'left_outer')

def join_window_df_with_stops_df_sql(table1, table2, sqlContext):
	""" 
	In lieu of the non-SQL version working, filters schedule for trip_ids in 
	windowed stops table (significantly reducing table size). That result is joined
	with windowed stops table, returning DataFrame.
	"""
	to_join =  sqlContext.sql('SELECT * FROM '+table1+' WHERE '+table1+'.trip_id IN (SELECT DISTINCT trip_id FROM '+table2+')')
	to_join.createOrReplaceTempView('subqueried')
	
	return sqlContext.sql('SELECT subqueried.*, '+table2+'.min_timestamp, '+table2+'.time_delta, '+table2+'.seq_delta, '+table2+'.`time/stop` FROM subqueried LEFT JOIN '+table2+' ON subqueried.trip_id='+table2+'.trip_id AND subqueried.stop_id='+table2+'.stop_id AND subqueried.stop_sequence='+table2+'.stop_sequence ORDER BY trip_id, stop_sequence')	


def df_with_window_backfill(df, window1, window2):
	""" 
	Provides new rows (generated from schedule) so that for each trip_id, intermediate
	stop_ids are included. For each of these new stop_ids, backfill via window time/stop. 
	Maps week of year, day of week, week of year.
	Returns a DataFrame where no time/stop value is less than 0 (for the case in which a trip erroneously
	'starts over.'
	"""
	last_stop_id = f.lag(df['stop_id']).over(window1)
	
	df = df.withColumn('prev_stop', last_stop_id)
	df = df.withColumn('filled_ts', f.first(df['time/stop'], ignorenulls=True).over(window2))
	df = df.withColumn('day', f.first(f.dayofweek(df['min_timestamp']), ignorenulls=True).over(window2))
	df = df.withColumn('hour', f.first(f.hour(df['min_timestamp']), ignorenulls=True).over(window2))
	df = df.withColumn('week', f.first(f.weekofyear(df['min_timestamp']), ignorenulls=True).over(window2))
	df = df.withColumn('diff_schedule_real', df['filled_ts'].cast('long') - df['schedule_delta'])	
	
	columns_to_return = ['trip_id', 'stop_id', 'prev_stop', 'stop_sequence', 'route_id', 'filled_ts', 'day', 'hour', 'week', 'diff_schedule_real']
	df = df.filter(df.filled_ts.isNotNull()) \
		.filter((df.filled_ts > 0)) \
		.filter((df.filled_ts != 6666.6666)) \
		.select(*columns_to_return)

	return df


if  __name__ == '__main__':
	scSpark = SparkSession.builder.appName('Stop to Stop').getOrCreate()
	scSpark.sparkContext.setLogLevel("ERROR")
	sqlCon = SQLContext(scSpark)

	stops_file = 'clean_stops.csv'
	trips_file = 'trips_agglom.csv'
	data_file = 'miniminiset.csv'

	if len(sys.argv) == 4:
		stops_file = sys.argv[1]
		data_file = sys.argv[2]
		trips_file = sys.argv[3]

	# Begin with a straightforward read from csv.	
	trips_df = scSpark.read.csv(trips_file, header=True, sep=',')
	
	# Stop times require casting. Convert to DataFrame
	stops_df = stop_times_to_df(stops_file, scSpark, trips_df)
	assert stops_df.schema.names is not None

	# Vehicle positions require dropping columns and casting. Convert to DataFrame.
	updates_df = vehicle_pos_to_df(data_file, scSpark)

	# Join positions updates DF with stops DF to determine stop sequence.
	# Return rows with minimum timestamp per stop_sequence.
	minned_df = pos_stop_initial_join(stops_df, updates_df)
	
	assert len(minned_df.head(1)) != 0
	
	# Save date in data as date for naming S3 file later.
	date = minned_df.first().asDict()['min_timestamp']
	
	# In case certain stop sequences occur twice, order Window by timestamp.
	window_look_back_one = Window \
		.partitionBy('trip_id') \
		.orderBy('min_timestamp')
	
	# Calculate time difference between row and row previous.
	window_df = df_with_window_time_diff(minned_df, window_look_back_one)

	# In order to perform spark.sql function, add table.
	window_df.createOrReplaceTempView('window_filled')

	window_look_back_one_order_seq = Window \
		.partitionBy('trip_id') \
		.orderBy('stop_sequence')

	# Determine approximate stop-to-stop time for the trip schedule.
	stops_df = add_schedule_difference(stops_df, window_look_back_one_order_seq)

	# Add table to sql context.
	stops_df.createOrReplaceTempView('stops')

	# Determine which trip_ids are represented, then join tables.
	stop_times_joined_df = join_window_df_with_stops_df_sql('stops', 'window_filled', sqlCon)	

	window_look_forward_many = Window \
		.partitionBy('trip_id') \
		.orderBy('trip_id', 'stop_sequence') \
		.rowsBetween(0, sys.maxsize)

	# Thanks to John Paton for forward fill (altered here for backward fill).
	# https://johnpaton.net/posts/forward-fill-spark/
	
	# Backfill time delta over nulls.
	df_backfilled = df_with_window_backfill(stop_times_joined_df, window_look_back_one_order_seq, window_look_forward_many)
	
	datestring = date.strftime("%Y-%m-%d")
	
	print('Saving to S3...')
	
	df_backfilled.write.save('s3a://stoptostop/preprocessed/'+datestring+'-preprocessed.csv', formate='csv', header=True)	
