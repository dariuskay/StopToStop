CREATE SCHEMA sts;
CREATE TABLE sts.data (`timestamp` timestamp, trip_id VARCHAR(50), \
	route_id VARCHAR(8), trip_start_date DATE, \
	latitude numeric(3,7), longitude numeric(3,7), stop_id numeric, \
	stop_sequence numeric, last_ts timestamp, last_seq numeric, \
	seq_delta numeric, time_delta time;