import csv
import timeit
import sys

def read(file, writer, subject):

	csv_reader = csv.reader(file, delimiter=',')
	next(csv_reader)
	if subject=='stop_times':
		for row in csv_reader:
			writer.writerow([row[0],row[1],row[3],row[4]])
	elif subject=='stops':
		for row in csv_reader:
			writer.writerow([row[0],row[1],row[3],row[4]])
	elif subject=='trips':
		for row in csv_reader:
			writer.writerow([row[0],row[2]])

def open_for_read(path, borough, writer, subject):
	with open((path+borough+'/'+subject+'.txt'), 'r',  newline='') as f:
		print(borough+' is starting.')
		read(f, writer, subject)
		print('-'*12)

def work(subject, boroughs, path):
	clean_file = open((subject+'_agglom.csv'), 'w')
	csv_writer = csv.writer(clean_file, delimiter=',')
	csv_writer.writerow(subject_dict[subject])

	for borough in boroughs:
		open_for_read(path, borough, csv_writer, subject)
	clean_file.close()


if __name__=='__main__':
	path = '../data/'

	if len(sys.argv) != 2:
		sys.exit('You must specify the path to the directory in which your borough zips reside.')
	else:
		path = sys.argv[1]

	subject_dict = {'stop_times': ['trip_id', 'stop_time' 'stop_id', 'stop_sequence'], \
					'stops': ['stop_id', 'stop_desc', 'lat', 'lon'], \
					'trips': ['route_id', 'trip_id']
					}

	subjects = ['stop_times', 'stops', 'trips']

	boroughs = ['bronx', 'brooklyn', 'staten', 'manhattan', 'queens']

	for each in subjects:
		work(each, boroughs, path)
