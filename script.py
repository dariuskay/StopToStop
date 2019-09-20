from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
import copy
from config import *

if __name__ == '__main__':

        # Creates SparkSession
        scSpark = SparkSession.builder.appName('Stop to Stop').getOrCreate()

        # Any csv's that start with '2019-09-', or all files from September 2019.
        # Each week is approximately 8,906,000 rows.

        stops_file = 'clean_stops.csv'
        data_file = 'miniminiset.csv'
        
        if len(sys.argv) > 1:
                stops_file = sys.argv[1]
                data_file = sys.argv[2]

        stop_times = scSpark.read.csv(stops_file, header=True, sep=',')

        print('-'*12)
        print('-'*12)
        print('STOP TIMES')
        stop_times.show()

        assert stop_times.schema.names is not None

        # Read csv, put into dataframe. Store in memory.
        sdfData = scSpark.read.csv(data_file, header=True, sep=',').cache()

        cols_to_drop = ('vehicle_id', 'vehicle_label', 'vehicle_license_plate', 'trip_start_time', \
                'bearing', 'speed', 'stop_status', 'occupancy_status', 'congestion_level', \
                'progress', 'block_assigned', 'dist_along_route', 'dist_from_stop')

        sdfData = sdfData.select([column for column in sdfData.columns if column not in cols_to_drop])

        # sdfData = sdfData.drop(sdfData.vehicle_id).collect()



        print(sdfData.schema.names)
        print('-'*12)
        print('UPDATES')
        sdfData.show()

        condition = ['trip_id', 'stop_id']
        
        joined_df = sdfData.join(stop_times, on=condition, how='inner')

        assert len(joined_df.head(1)) != 0

        print('JOINED_DF')
        print('-'*12)

        joined_df.show()

        print(joined_df.schema.names)
        print('-'*12)

        minned_df = sdfData.groupby('trip_id', 'stop_id') \
                .agg(f.min('timestamp')).alias('min_timestamp').orderBy('trip_id', 'stop_id')

        _schema = copy.deepcopy(X.schema)
        minned_twinned = X.rdd.zipWithIndex().toDF(_schema)

        # Thanks to @christinebuckler for the Spark dataframe deepcopy.

        ultimate_df = sqlContext.sql("SELECT max(minned_twinned.min_timestamp) as last_ts, \
                max(minned_twinned.stop_sequence) as last_seq, minned_df.* \
                FROM minned_df LEFT JOIN minned_twinned ON minned_df.trip_id = minned_twinned.trip_id \
                WHERE minned_twinned.stop_sequence < minned_df.stop_sequence") # Still have my doubts that this can work...


        ultimate_df.withColumn('time_delta', ultimate_df.timestamp - ultimate_df.last_ts)
        ultimate_df.withColumn('seq_delta', ultimate_df.sequence - ultimate_df.last_seq)

        # WHAT NOW?? Seems like it's time to do a left join with stops again, but how to map the averages?

        # After that, map days of the week and time windows.

        ultimate_df.show()


        properties = {
            "user": "postgres",
            "password": "postgres"
        }

        df.write.jdbc(url='jdbc:postgresql://database-4.ceegl9gaalwk.us-west-2.rds.amazonaws.com:5432/metrics', \
                table="sts", mode='overwrite', properties=properties)
