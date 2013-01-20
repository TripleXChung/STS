import os
import redis
import time
import datetime
import collections
from multiprocessing import Process, Pipe
import cPickle as pickle
import simplejson as json

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

datas = {}
totals = {}
persistent = False

class STS():
	def __init__(self):
		self.client = redis.Redis(host='localhost', port=6379, db=0)

	def publish_avg(self, server, timestamp, avg):
		msg = json.dumps([server, timestamp, avg])
		self.client.publish('avg', msg)
		profile()
		#print msg

	def load_data(self, server):
		global datas
		if(not datas.has_key(server)):
			datas[server] = collections.deque([])
			totals[server] = 0
		return datas[server]

	def load_persistent_data(self, server):
		if(datas.has_key(server)):
			return datas[server]
		l = self.client.llen(server)
		res = collections.deque([])
		total = 0
		if(l > 0):
			tmps = self.client.lrange(server, 0, l)
			for tmp in tmps:
				obj = pickle.loads(tmp)
				res.append(obj)
				total = total + obj[1]
		datas[server] = res
		totals[server] = total

		return res

	def apply_value(self, server, timestamp, value):
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
			data = self.load_persistent_data(server)
		else:
			data = self.load_data(server)

		data.append(obj)
		totals[server] = totals[server] + obj[1]
		total = totals[server]

		if(persistent):
			self.client.rpush(server, pickle.dumps(obj))
		
		last_timestamp = timestamp
		while True:
			obj = data[0]
			timestamp = obj[0]
			if(last_timestamp - timestamp > (30 * 24 * 3600)):
				data.popleft()
				if(persistent):
					self.client.lpop(server)
				totals[server] = totals[server] - obj[1]
				total = totals[server]
			else:
				break
		count = len(datas)
		avg = total / count
		#print server_string + '--' + str(count)
		self.publish_avg(server, last_timestamp, avg)

	def handle_request(self, req):
		server = req[0]
		timestamp = req[1]
		value = req[2]
		self.apply_value(server, timestamp, value)

def child_process(q, sts):
	while True:
		req = q.recv()
		sts.handle_request(req)

process_num = 4

if __name__ == '__main__':
	process_list = []
	queue_list = []
	for i in xrange(process_num):
		parent_conn, child_conn = Pipe()
		p = Process(target=child_process, args=(child_conn, STS()))
		p.start()
		queue_list.append(parent_conn)
		process_list.append(p)
	#cnt = 0
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
			if(type(req) != type([])):
				continue
			if(len(req) != 3):
				continue
			server = req[0]
			if(type(server) != int):
				continue
			idx = server % process_num
			queue_list[idx].send(req)
			#threads[cnt % thread_num].enqueue(req)
			#cnt = cnt + 1
			#handle_request(req)
		except Exception,e:
			print e
