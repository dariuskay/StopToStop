from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql import types
import pyspark.sql.functions as f
import sys
import copy
# from config import *
import config

if __name__ == '__main__':

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

        stop_times = scSpark.read.csv(stops_file, header=True, sep=',')
	stop_times = stop_times.withColumn('stop_sequence', stop_times['stop_sequence'].cast('double'))

        print('-'*12)
        print('-'*12)
        print('STOP TIMES')
        stop_times.show()

        assert stop_times.schema.names is not None
	
	print('='*20)
	print('STOP TIMES SCHEMA')
	print(stop_times.schema.names)
	print('='*20)

        # Read csv, put into dataframe. Store in memory.
        sdfData = scSpark.read.csv(data_file, header=True, sep=',').cache()

        cols_to_drop = ('vehicle_id', 'vehicle_label', 'vehicle_license_plate', 'trip_start_time', \
                'bearing', 'speed', 'stop_status', 'occupancy_status', 'congestion_level', \
                'progress', 'block_assigned', 'dist_along_route', 'dist_from_stop')

        sdfData = sdfData.select([column for column in sdfData.columns if column not in cols_to_drop])
	
	sdfData = sdfData.withColumn('timestamp', sdfData['timestamp'].cast('timestamp'))
	
        # sdfData = sdfData.drop(sdfData.vehicle_id).collect()


	print('='*20)
        print('UPDATES SCHEMA')
        print(sdfData.schema.names)
        print('='*20)

        print('-'*12)
        print('UPDATES')
        sdfData.show()

        condition = ['trip_id', 'stop_id']
        
        # joined_df = sdfData.join(stop_times, on=condition, how='inner')
	
       	minned_df = sdfData.join(stop_times, ['trip_id', 'stop_id']) \
			.groupby('trip_id', 'stop_id', 'stop_sequence') \
			.agg(f.min('timestamp').alias('min_timestamp')) \
			.orderBy('trip_id', 'stop_id') 
			# .select(f.col("min(timestamp)").alias("min_timestamp"))
	# assert len(joined_df.head(1)) != 0

        # print('JOINED_DF')
        # print('-'*12)

	# properties = {
          #  "user": "postgres",
           # "password": "postgres"
       # }

        #joined_df.show()

	# joined_df.write.jdbc(url=config.url, \
          #      table="sts", mode='overwrite', properties=properties)	



        # print(joined_df.schema.names)
        # print('-'*12)

        # minned_df = joined_df.groupby('trip_id', 'stop_id', 'stop_sequence') \
          #      .agg(f.min('timestamp').alias('min_timestamp')).orderBy('trip_id', 'stop_id')

	assert len(minned_df.head(1)) != 0

	print('MINNED DF')
	print('='*12)
	print('='*12)
	minned_df.show()
	
	print('Printed schema')
	minned_df.printSchema()
	
	# In order to use a physical window function, we sort by trip_id and timestamp

	# columns = ['trip_id', 'min_timestamp']
	# minned_sorted = minned_df.orderBy(columns, ascending=[0, 1])

	# assert len(minned_sorted.head(1)) != 0

	# print('MINNED SORTED')
	# print('='*12)
	# minned_sorted.show()

	window_spec = Window \
		.partitionBy('trip_id') \
		.orderBy('min_timestamp')
	
	time_delta = minned_df.min_timestamp.cast('long') - f.lag(minned_df.min_timestamp).over(window_spec).cast('long')

	seq_delta = minned_df.stop_sequence - f.lag(minned_df.stop_sequence).over(window_spec)

	time_per_stop = time_delta / seq_delta

	print('MINNED WINDOW')
	print('='*12)
	print('='*12)

	min_win = minned_df.select(minned_df['trip_id'], \
		minned_df['stop_id'], \
		minned_df['stop_sequence'], \
		minned_df['min_timestamp'], \
		time_delta.alias('time_delta'), \
		seq_delta.alias('seq_delta'), \
		time_per_stop.alias('time/stop'))

	min_win.show()

	stop_times_join = stop_times.join(min_win, ['trip_id', 'stop_id', 'stop_sequence'], 'left_outer')
	stop_times_join = stop_times_join.withColumn('prev_stop', f.lag(stop_times_join.stop_id).over(window_spec))
	
	new_window = Window \
		.partitionBy('trip_id') \
		.orderBy('min_timestamp') \
		.rowsBetween(0, sys.maxsize)

	# Thanks to John Paton for forward fill (altered here for backward fill).
	# https://johnpaton.net/posts/forward-fill-spark/

	filled = f.last(stop_times_join['time/stop'], ignorenulls=True).over(new_window)

	stop_times_join = stop_times_join.withColumn('filled', filled)

	stop_times_join.withColumn('day', f.dayofweek(stop_times_join['min_timestamp']))
	
	stop_times_join.withColumn('hour', f.hour(stop_times_join['min_timestamp']))

	print('JOIN')
	stop_times_join.show()

        # After that, map days of the week and time windows.

        # ultimate_df.show()


        properties = {
            "user": "postgres",
            "password": "postgres"
        }
	
	

        stop_times_join.write.jdbc(url=config.url, \
                table="data", mode='overwrite', properties=properties)
