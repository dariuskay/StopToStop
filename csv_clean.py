import csv
import timeit

clean_file = open('clean_stops.csv', mode='w')
csv_writer = csv.writer(clean_file, delimiter=',')


def read_stops(file, writer):

	csv_reader = csv.reader(file, delimiter=',')

	prev_stop = 'NONE'
	trip = 'NONE'
	next(csv_reader)
	writer.writerow(['trip_id', 'arrival_time', 'stop_id', 'prev_id'])
	for row in csv_reader:
		if row[0] != trip:
			row[4] = row[3]
			trip = row[0]
			prev_stop = row[3]
			writer.writerow([row[0], row[1], row[3], row[3]])
		else:
			writer.writerow([row[0], row[1], row[3], prev_stop])
			prev_stop = row[3]

with open('google_transit_bronx/stop_times.txt', newline='', mode='r') as bronx:

	print('BRONX is starting.')
	start = timeit.timeit()
	read_stops(bronx, csv_writer)
	end = timeit.timeit()
	print(f'BRONX has finished. It took {start - end} seconds.')
	print('-'*12)


with open('google_transit_staten_island/stop_times.txt', newline='', mode='r') as staten:

	print('STATEN ISLAND is starting.')
	start = timeit.timeit()
	read_stops(staten, csv_writer)
	end = timeit.timeit()
	print(f'STATEN ISLAND has finished. It took {start - end} seconds.')
	print('-'*12)

with open('google_transit_brooklyn/stop_times.txt', newline='', mode='r') as brooklyn:

	print('BROOKLYN is starting.')
	start = timeit.timeit()
	read_stops(brooklyn, csv_writer)
	end = timeit.timeit()
	print(f'BROOKLYN has finished. It took {start - end} seconds.')
	print('-'*12)


with open('google_transit_manhattan/stop_times.txt', newline='', mode='r') as manhattan:

	print('MANHATTAN is starting.')
	start = timeit.timeit()
	read_stops(manhattan, csv_writer)
	end = timeit.timeit()
	print(f'MANHATTAN has finished. It took {start - end} seconds.')
	print('-'*12)

with open('google_transit_queens/stop_times.txt', newline='', mode='r') as queens:

	print('QUEENS is starting.')
	start = timeit.timeit()
	read_stops(queens, csv_writer)
	end = timeit.timeit()
	print(f'QUEENS has finished. It took {start - end} seconds.')
	print('-'*12)


clean_file.close()

