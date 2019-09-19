from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as f

# import pandas as pd


# stop_udf = udf(lambda arr: recent_stop(arr[12], arr[1]), StringType())
# # Returns sequence number of current stop_id within the route
# def get_sequence(stop_id: str, trip_id: str, stop_times) -> int:
#         return stop_times.loc[stop_times['stop_id'] == stop_id & stop_times['trip_id'] == trip_id, 'stop_sequence'].iloc[0]

# # Determined by a stop_sequence and route_id, retrieve the stop_id for the previous stop in the route
# def get_last_stop_id(trip_id: str, sequence: int, stop_times) -> str:
#         return stop_times.loc[stop_times['trip_id'] == trip_id & stop_times['stop_sequence'] == sequence - 1, 'stop_id'].iloc[0]

# # Within the main updates dataframe, identify the earliest timestamp for the previous stop.
# def get_last_stop_ts(stop_id: str, trip_id: str, updates) -> str:
#         return min(updates.loc[updates['trip_id'] == trip_id & updates['stop_sequence']])


# def recent_stop(stop_id: str, trip_id: str, stop_times) -> str:
#         sequence = get_sequence(stop_id, trip_id, stop_times)
#         return get_last_stop_id(trip_id, sequence, stop_times)

if __name__ == '__main__':
        scSpark = SparkSession.builder.appName('Stop to Stop').getOrCreate()

        # Any csv's that start with '2019-09-', or all files from September 2019
        # Each week is approximately 8,906,000 rows

        # data_file = 'hdfs://ip-10-0-0-6.us-west-2.compute.internal:9000/dfs-data/2019-09-01.csv'

        # data_file = '2019-09-01-bus-positions.csv'
        data_file = 'miniminiset.csv'

        stop_times = scSpark.read.csv('clean_stops.csv', header=True, sep=',')

        print('-'*12)
        print('-'*12)
        print('STOP TIMES')
        stop_times.show()

        assert stop_times.schema.names is not None

        # Read csv, put into dataframe
        sdfData = scSpark.read.csv(data_file, header=True, sep=',').cache()

        condition = ['trip_id', 'stop_id']

        cols_to_drop = ('vehicle_id', 'vehicle_label', 'vehicle_license_plate', 'trip_start_time', \
                'bearing', 'speed', 'stop_status', 'occupancy_status', 'congestion_level', \
                'progress', 'block_assigned', 'dist_along_route', 'dist_from_stop')

        sdfData = sdfData.select([column for column in sdfData.columns if column not in cols_to_drop])

        # sdfData = sdfData.drop(sdfData.vehicle_id).collect()

        print(sdfData.schema.names)
        print('-'*12)
        print('UPDATES')
        sdfData.show()

        # for each in cols_to_drop:
        #         sdfData = sdfData.drop(each).collect()


        
        joined_df = sdfData.join(stop_times, on=condition, how='inner')

        print('JOINED_DF')
        print('-'*12)

        joined_df.show()

        print(joined_df.schema.names)
        print('-'*12)

        minned_df = sdfData.groupby('trip_id', 'stop_id').agg(f.min('timestamp')).alias('min_timestamp').orderBy('trip_id', 'stop_id')

        minned_df.show()

        condition = [joined_df.trip_id == minned_df.trip_id, joined_df.prev_id == minned_df.stop_id]

        ultimate_df = joined_df.join(minned_df, on=condition)

        ultimate_df.show()

        # ultimate_df.printSchema()

        # ultimate_df.show()

        # sdfData = sdfData.withColumn('last_stop_id', recent_stop(sdfData.stop_id, sdfData.trip_id, stop_times))
        # joined_df.show()

        # sdfBronx = scSpark.read.csv('data/bronx/stop_times.txt', header=True, sep=',').cache()        
        # sdfBronx.show()


        # assert sdfData is not None