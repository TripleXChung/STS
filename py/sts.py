import os
import redis
import cPickle as pickle
import simplejson as json

client = redis.Redis(host='localhost', port=6379, db=0)

def publish_avg(server, timestamp, avg):
	msg = json.dumps([server, timestamp, avg])
	client.publish('avg', msg)
	#print msg

def apply_value(server, timestamp, value):
	if(type(server) != int):
		return
	server_string = "%2.2x"%(server)

	if(type(timestamp) != int):
		return
	if(type(value) != int):
		return
	obj = [timestamp, value]
	
	base_dir = 'data/' + server_string[:1]
	if(os.path.exists(base_dir) == False):
		os.mkdir(base_dir)
	base_dir = base_dir + '/' + server_string[1:2]
	if(os.path.exists(base_dir) == False):
		os.mkdir(base_dir)

	file_path = base_dir + '/' + server_string
	try:
		fd = open(file_path, 'r')
		data = pickle.load(fd)
		fd.close()
	except Exception,e:
		data = []
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
	print server_string + '--' + str(count)
	publish_avg(server, last_timestamp, avg)
	try:
		fd = open(file_path, 'w')
		pickle.dump(data, fd)
		fd.close()
	except Exception,e:
		print e

def handle_request(req):
	if(type(req) != type([])):
		return
	if(len(req) != 3):
		return
	server = req[0]
	timestamp = req[1]
	value = req[2]
	apply_value(server, timestamp, value)

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
			handle_request(req)
		except Exception,e:
			print e