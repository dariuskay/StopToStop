import csv
import timeit

clean_file = open('clean_stops.csv', mode='w')
csv_writer = csv.writer(clean_file, delimiter=',')
csv_writer.writerow(['trip_id', 'stop_id', 'stop_sequence'])



def read_stops(file, writer):

	csv_reader = csv.reader(file, delimiter=',')
	next(csv_reader)
	for row in csv_reader:
		writer.writerow([row[0],row[3],row[4]])

with open('../data/bronx/stop_times.txt', newline='', mode='r') as bronx:

	print('BRONX is starting.')
	start = timeit.timeit()
	read_stops(bronx, csv_writer)
	end = timeit.timeit()
	# print(f'BRONX has finished. It took {start - end} seconds.')
	print('-'*12)


with open('../data/staten/stop_times.txt', newline='', mode='r') as staten:

	print('STATEN ISLAND is starting.')
	start = timeit.timeit()
	read_stops(staten, csv_writer)
	end = timeit.timeit()
	# print(f'STATEN ISLAND has finished. It took {start - end} seconds.')
	print('-'*12)

with open('../data/brooklyn/stop_times.txt', newline='', mode='r') as brooklyn:

	print('BROOKLYN is starting.')
	start = timeit.timeit()
	read_stops(brooklyn, csv_writer)
	end = timeit.timeit()
	# print(f'BROOKLYN has finished. It took {start - end} seconds.')
	print('-'*12)


with open('../data/manhattan/stop_times.txt', newline='', mode='r') as manhattan:

	print('MANHATTAN is starting.')
	start = timeit.timeit()
	read_stops(manhattan, csv_writer)
	end = timeit.timeit()
	# print(f'MANHATTAN has finished. It took {start - end} seconds.')
	print('-'*12)

with open('../data/queens/stop_times.txt', newline='', mode='r') as queens:

	print('QUEENS is starting.')
	start = timeit.timeit()
	read_stops(queens, csv_writer)
	end = timeit.timeit()
	# print(f'QUEENS has finished. It took {start - end} seconds.')
	print('-'*12)


clean_file.close()

