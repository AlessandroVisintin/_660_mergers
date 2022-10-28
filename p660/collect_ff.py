from p660.utils import parse_user_object
from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import get_friend_ids
from WebUtils.threaded_twitter import get_friends
from WebUtils.threaded_twitter import get_follower_ids
from WebUtils.threaded_twitter import get_followers
from WebUtils.threaded_twitter import lookup_users
from JSONWrap.utils import load

import time
from queue import Queue, PriorityQueue
from threading import Thread


MAX_DELTA = 60 * 60 * 24
OUT = 'out/p660'
CFG = 'config/p660'
DB_OUT = f'{OUT}/p660.db'
DB_CFG = f'{CFG}/p660.yaml'
CL_CFG = f'{CFG}/clusters.yaml'
CR_CFG = 'config/WebUtils/twitterapi_cred.yaml'

# start threads
apis = {
	get_friend_ids : [Queue(), Queue(), []],
	get_friends: [Queue(), Queue(), []],
	get_followers : [Queue(), Queue(), []],
	get_follower_ids: [Queue(), Queue(), []],
	lookup_users: [Queue(), Queue(), []],
	}
for name, k in load(CR_CFG).items():
	for func, v in apis.items():
		t = Thread(target=func, args=(k, 'user', v[0], v[1]))
		t.start()
		v[2].append(t)

# prepare mergers info
mergers = {}
for j,u in load(CL_CFG).items():
	for v in u[1]:
		try:
			mergers[v].update(set(u[0]))
		except KeyError:
			mergers[v] = set(u[0])

# prepare database
p660 = SQLite(DB_OUT, config=DB_CFG)
p660.fetch(name='create_collected')
for merger,accounts in mergers.items():
	print(merger)
	for account in accounts:
		print(account)
		
		# prepare FF table
		p660.fetch(name='create_ff', format={'t':account})
				
		start = time.time()
		pqueue = PriorityQueue()
		rows = [[],[]]
		while time.time() - start < MAX_DELTA:
			if pqueue.qsize() == 0:
				p660.add_index(
					f'{account}_Fwsid1', f'{account}_Fws', 'id1', if_not_exists=True)
				p660.add_index(
					f'{account}_Fwsid2', f'{account}_Fws', 'id2', if_not_exists=True)
				p660.add_index(
					f'{account}_FFid1', f'{account}_FF', 'id1', if_not_exists=True)
				p660.add_index(
					f'{account}_FFid2', f'{account}_FF', 'id2', if_not_exists=True)
				q = (
					f'CREATE TEMPORARY TABLE {account}_remainingFws AS '
					f'SELECT a.id2 AS id FROM {account}_Fws a '
					f'LEFT JOIN Collected b ON a.id2 = b.id '
					f'WHERE b.id IS NULL;'
					)
				p660.fetch(query=q)
				q = (
					f'CREATE TEMPORARY TABLE {account}_remainingFF AS '
					f'SELECT b.id1, b.id2 FROM {account}_remainingFws a '
					f'INNER JOIN {account}_FF b ON a.id = b.id1 '
					f'INNER JOIN {account}_remainingFws c ON b.id2 = c.id;'
					)
				p660.fetch(query=q)
				q = (
					f'SELECT a.id, CASE WHEN b.e < c.e THEN b.e ELSE c.e END AS deg '
					f'FROM {account}_remainingFws a '
					f'LEFT JOIN ('
					f'SELECT id1, COUNT(*) AS e FROM {account}_remainingFF '
					f'GROUP BY id1) b ON a.id = b.id1 '
					f'LEFT JOIN ('
					f'SELECT id2, COUNT(*) AS e FROM {account}_remainingFF '
					f'GROUP BY id2) c ON a.id = c.id2 '
					f'ORDER BY deg DESC '
					f'LIMIT 1000;'
					)
				for row in p660.fetch(query=q):
					p = 2 if row[1] is None else 1 / row[1]
					pqueue.put((p,row[0]))
				p660.fetch(query=f'DROP TABLE {account}_remainingFws;')
				p660.fetch(query=f'DROP TABLE {account}_remainingFF;')
				if pqueue.qsize() == 0:
					break
				p660.drop('index', f'{account}_Fwsid1')
				p660.drop('index', f'{account}_Fwsid2')
				p660.drop('index', f'{account}_FFid1')
				p660.drop('index', f'{account}_FFid2')
			
			p, uid = pqueue.get()
			apis[lookup_users][0].put([uid])
			data = apis[lookup_users][1].get()
			if data is None:
				continue
			
			obj = parse_user_object(data[0])
			print('\t', round(p,6), obj[0], obj[14])
			
			rows[0].append(obj)
			if not obj[13]: # if not protected
				if obj[6] > 0: # if followers > 0
					if obj[6] <= 200:
						apis[get_followers][0].put((uid, -1, 200))
						data = apis[get_followers][1].get()
						rows[0].extend(parse_user_object(x) for x in data['users'])
						rows[1].extend((uid, x['id'], None, None) for x in data['users'])
					else:
						apis[get_follower_ids][0].put((uid,-1,5000))
						data = apis[get_follower_ids][1].get()
						rows[1].extend((uid, x, None, None) for x in data['ids'])
				if obj[7] > 0: # if friends > 0
					if obj[7] <= 200:
						apis[get_friends][0].put((uid, -1, 200))
						data = apis[get_friends][1].get()
						rows[0].extend(parse_user_object(x) for x in data['users'])
						rows[1].extend((x['id'], uid, None, None) for x in data['users'])
					else:
						apis[get_friend_ids][0].put((uid,-1,5000))
						data = apis[get_friend_ids][1].get()
						rows[1].extend((uid, x, None, None) for x in data['ids'])

			print('\t\tinserting')
			p660.fetch(name='insert_Users', params=rows[0])
			p660.fetch(name='insert_ff', format={'t':account}, params=rows[1])
			p660.fetch(name='insert_collected', params=[(uid,)])

# close db
del p660

# close threads
for func, v in apis.items():
	for _ in v[2]:
		v[0].put(None)
	for t in v[2]:
		t.join()
