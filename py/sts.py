import os
import redis
import time
import datetime
import collections
import cPickle as pickle
import simplejson as json

datas = {}
totals = {}
persistent = False
client = redis.Redis(host='localhost', port=6379, db=0)

def publish_avg(server, timestamp, avg):
	global client
	msg = json.dumps([server, timestamp, avg])
	client.publish('avg', msg)
	#print msg

def load_data(server):
	global datas
	if(not datas.has_key(server)):
		datas[server] = collections.deque([])
		totals[server] = 0
	return datas[server]

def load_persistent_data(server):
	global client
	if(datas.has_key(server)):
		return datas[server]
	l = client.llen(server)
	res = collections.deque([])
	total = 0
	if(l > 0):
		tmps = client.lrange(server, 0, l)
		for tmp in tmps:
			obj = pickle.loads(tmp)
			res.append(obj)
			total = total + obj[1]
	totals[server] = total
	datas[server] = res
	return res

def apply_value(server, timestamp, value):
	global client
	global persistent
	if(type(server) != int):
		return
	#server_string = "%2.2x"%(server)

	if(type(timestamp) != int):
		return
	if(type(value) != int):
		return
	obj = [timestamp, value]
	
	if(persistent):
		data = load_persistent_data(server)
	else:
		data = load_data(server)

	data.append(obj)
	totals[server] = totals[server] + obj[1]
	total = totals[server]

	if(persistent):
		client.rpush(server, pickle.dumps(obj))
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
				client.lpop(server)
			totals[server] = totals[server] - obj[1]
			total = totals[server]
		else:
			break
	count = len(datas)
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
