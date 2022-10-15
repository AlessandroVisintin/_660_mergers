"""

Collect followers lists of the followers of a user.
The collection is limited to accounts <= 5000 followers.

"""

from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import get_followers_ids
from WebUtils.threaded_twitter import get_followers
from WebUtils.threaded_twitter import lookup_users
from JSONWrap.utils import load

import time
from queue import Queue, PriorityQueue
from threading import Thread, Event


MAX_DELTA = 60 * 60 * 8
PATH = 'out/p660'
DB_CONFIG = 'config/p660/p660.yaml'
API_CRED = 'config/WebUtils/twitterapi_cred.yaml'

# load database of users
db = SQLite(DB_CONFIG)

# load authentication	
credentials = load(API_CRED)

# create threads for accounts < 200 followers and < 5000 followers
end = Event()
in_fws = Queue()
out_fws = Queue()
in_look = Queue()
out_look = Queue()

apis = {
	
	'users' : [
		Thread(
			target=get_followers,
			args=(v, 'user', end, in_fws, out_fws)
			) for k,v in credentials.items()],
	'ids' : [
		Thread(
			target=get_followers_ids,
			args=(v, 'user', end, in_fws, out_fws)
			) for k,v in credentials.items()],
	'lookup' : [
		Thread(
			target=lookup_users,
			args=(v, 'user', end, in_look, out_look)
			) for k,v in credentials.items()]
	
	}

# start threads
for k,v in apis.items():
	for t in v:
		t.start()

# get userids
with open(f'{PATH}/usernames.txt', 'r') as f:
	usernames = '","'.join([line.strip() for line in f])
	query = f'SELECT id,username FROM Users WHERE username IN ("{usernames}");'
	db.add_query('select', 'uname2uid', query)
	USERS = [row for row in db.select('uname2uid')]


for userid, username in USERS:
	print(userid ,username)

	print('Loading priority queue...')
	pset = set()
	pqueue = PriorityQueue()	
	query = f'SELECT Users_id2 FROM IsFollowedBy WHERE Users_id1 = {userid};'
	db.add_query('select', 'get_follows', query)
	for row in db.select('get_follows'):
		pqueue.put((2,row[0]))

	start = time.time()
	while pqueue.qsize() > 0 and time.time() - start < MAX_DELTA:
		p, uid = pqueue.get()
		in_look.put([uid])
		data = out_look.get()
		
		uid = data[0]['id']
		usr = data[0]['screen_name']
		fws = data[0]['followers_count']
		prv = data[0]['protected']
		
		print('\t', p, uid, usr)
		
		print('\t\tinserting user')
		db.insert('Users', [(uid,usr,fws,prv)])
		if prv or fws == 0 or uid in pset:
			continue
	
		pset.add(uid)
		if fws < 200:
			in_fws.put((data[0]['screen_name'], -1, 200))
			data = [u['id'] for u in out_fws.get()['users']]
		else:
			in_fws.put((data[0]['screen_name'], -1, 5000))
			data = out_fws.get()['ids']
 		
		print('\t\tinserting followers')
		db.insert('IsFollowedBy', [(uid,x) for x in data])

		uids = '","'.join([str(x) for x in data])
		query = f'SELECT Users_id2 FROM IsFollowedBy WHERE Users_id1 = "{userid}" AND Users_id2 IN ("{uids}");'
		db.add_query('select', 'intersect', query)
		intersection = [row[0] for row in db.select('intersect')]
		for uid in intersection:
			pqueue.put((1 / len(intersection), uid))
		
		with open(f'{PATH}/log.txt', 'w') as f:
			f.write(f'{userid}\t{username}')

# close threads
end.set()
for k,v in apis.items():
	for t in v:
		in_fws.put(None)
		in_look.put(None)
for k,v in apis.items():
	for t in v:
		t.join()
