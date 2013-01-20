import os
import redis
import time
import datetime
import collections
import cPickle as pickle
import simplejson as json

datas = {}
persistent = True
client = redis.Redis(host='localhost', port=6379, db=0)

def publish_avg(server, timestamp, avg):
	global client
	msg = json.dumps([server, timestamp, avg])
	client.publish('avg', msg)
	#print msg

def load_data(server_string):
	global datas
	if(not datas.has_key(server_string)):
		datas[server_string] = collections.deque([])
	return datas[server_string]

def load_persistent_data(server_string):
	global client
	if(datas.has_key(server_string)):
		return datas[server_string]
	l = client.llen(server_string)
	res = collections.deque([])
	if(l > 0):
		tmps = client.lrange(server_string, 0, l)
		for tmp in tmps:
			obj = pickle.loads(tmp)
			res.append(obj)
	datas[server_string] = res
	return res

def apply_value(server, timestamp, value):
	global client
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
	if(persistent):
		client.rpush(server_string, pickle.dumps(obj))
	"""new_data = []
	last_timestamp = timestamp
	for obj in data:
		timestamp = obj[0]
		if(last_timestamp - timestamp < (30 * 24 * 3600)):
			new_data.append(obj)
	data = new_data"""
	last_timestamp = timestamp
	while True:
		obj = data[0]
		timestamp = obj[0]
		if(last_timestamp - timestamp > (30 * 24 * 3600)):
			data.popleft()
			if(persistent):
				client.lpop(server_string)
		else:
			break

	count = 0
	total = 0
	for obj in data:
		total = total + obj[1]
		count = count + 1
	avg = total / count
	#print server_string + '--' + str(count)
	publish_avg(server, last_timestamp, avg)

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
	subclient = redis.Redis(host='localhost', port=6379, db=0)
	pubsub = subclient.pubsub()
	pubsub.psubscribe('event')
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