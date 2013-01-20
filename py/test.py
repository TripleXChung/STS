import os
import redis
import cPickle as pickle
import simplejson as json

client = redis.Redis(host='localhost', port=6379, db=0)

if __name__ == '__main__':
	for i in xrange(3000):
		for server in xrange(100000):
			client.publish('event', json.dumps([server, i, 15]))
	