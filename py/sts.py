import os
import redis
import time
import datetime
import cPickle as pickle
import simplejson as json

datas = {}
persistent = False
client = redis.Redis(host='localhost', port=6379, db=0)

def publish_avg(server, timestamp, avg):
	global client
	msg = json.dumps([server, timestamp, avg])
	client.publish('avg', msg)
	#print msg

def get_file_path(server_string):
	base_dir = 'data/' + server_string[:1]
	if(os.path.exists(base_dir) == False):
		os.mkdir(base_dir)
	base_dir = base_dir + '/' + server_string[1:2]
	if(os.path.exists(base_dir) == False):
		os.mkdir(base_dir)

	file_path = base_dir + '/' + server_string
	return file_path

def load_data(server_string):
	global datas
	if(datas.has_key(server_string)):
		return datas[server_string]
	return []

def store_data(data, server_string):
	global datas
	datas[server_string] = data

def load_persistent_data(server_string):
	file_path = get_file_path(server_string)
	try:
		fd = open(file_path, 'r')
		data = pickle.load(fd)
		fd.close()
	except Exception,e:
		data = []
	return data

def store_persistent_data(data, server_string):
	file_path = get_file_path(server_string)
	try:
		fd = open(file_path, 'w')
		pickle.dump(data, fd)
		fd.close()
	except Exception,e:
		print e

def apply_value(server, timestamp, value):
	global persistent
	if(type(server) != int):
		return
	server_string = "%2.2x"%(server)

	if(type(timestamp) != int):
		return
	if(type(value) != int):
		return
	obj = [timestamp, value]
	
	if(persistent):
		data = load_persistent_data(server_string)
	else:
		data = load_data(server_string)

	data.append(obj)

	new_data = []
	last_timestamp = timestamp
	for obj in data:
		timestamp = obj[0]
		if(last_timestamp - timestamp < (30 * 24 * 3600)):
			new_data.append(obj)
	data = new_data

	count = 0
	total = 0
	for obj in data:
		total = total + obj[1]
		count = count + 1
	avg = total / count
	#print server_string + '--' + str(count)
	publish_avg(server, last_timestamp, avg)

	if(persistent):
		store_persistent_data(data, server_string)
	else:
		store_data(data, server_string)

def handle_request(req):
	if(type(req) != type([])):
		return
	if(len(req) != 3):
		return
	server = req[0]
	timestamp = req[1]
	value = req[2]
	apply_value(server, timestamp, value)

profile_count = 0 
profile_timestamp = 0

def profile():
	global profile_count, profile_timestamp
	profile_count = profile_count + 1
	timestamp = time.mktime(datetime.datetime.now().timetuple())
	timediff = timestamp - profile_timestamp
	if(timediff < 1):
		return
	profile_timestamp = timestamp
	print (profile_count / timediff)
	profile_count = 0

if __name__ == '__main__':
	pubsub = client.pubsub()
	pubsub.psubscribe('event')
	if(os.path.exists('data') == False):
		os.mkdir('data')
	while True:
		msg = pubsub.listen().next()
		channel = msg['channel']
		if(channel != 'event'):
			continue
		try:
			req = json.loads(msg['data'])
			profile()
			handle_request(req)
		except Exception,e:
			print e