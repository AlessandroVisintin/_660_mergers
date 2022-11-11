from p660.utils import parse_user_object
from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import get_friend_ids
from WebUtils.threaded_twitter import get_friends
from WebUtils.threaded_twitter import get_follower_ids
from WebUtils.threaded_twitter import get_followers
from WebUtils.threaded_twitter import lookup_users
from JSONWrap.utils import load

import sys
import time
from queue import Queue
from threading import Thread


MAX_DELTA = 60 * 60 * 24
OUT = 'out/p660'
CFG = 'config/p660'

try:
	LIMIT = int(sys.argv[1])
except IndexError:
	LIMIT = 1000

try:
	RAND = int(sys.argv[2])
except IndexError:
	RAND = 250

# start threads
apis = {
	get_friend_ids : [Queue(), Queue(), []],
	get_friends: [Queue(), Queue(), []],
	get_followers : [Queue(), Queue(), []],
	get_follower_ids: [Queue(), Queue(), []],
	lookup_users: [Queue(), Queue(), []],
	}
for name, k in load('config/WebUtils/twitterapi_cred.yaml').items():
	for func, v in apis.items():
		t = Thread(target=func, args=(k, 'user', v[0], v[1]))
		t.start()
		v[2].append(t)

# prepare mergers info
clusters = load(f'{CFG}/clusters.yaml')

# load done
try:
	with open(f'{OUT}/done.txt', 'r') as f:
		done = set([x.strip() for x in f])
except FileNotFoundError:
	done = set()

# collect
for cluster, values in clusters.items():
	print(cluster)
	
	if cluster in done:
		print('already done')
		continue
	
	db = SQLite(f'{OUT}/{cluster}.db', config=f'{CFG}/p660.yaml')
	
	print('Creating Fws indexes', end='')
	db.add_index('Fws_id1', 'Fws', 'id1', if_not_exists=True)
	print('.', end='')
	db.add_index('Fws_id2', 'Fws', 'id2', if_not_exists=True)
	print('.', end='')

	for account in values[0]:
		print(account)

		start = time.time()
		tovisit = set()
		rows = [[],[],[]]
		while time.time() - start < MAX_DELTA:
			if len(tovisit) == 0:
				print('\tFill tovisit', end='')
				db.add_index('FF_id1', 'FF', 'id1', if_not_exists=True)
				print('.', end='')
				db.add_index('FF_id2', 'FF', 'id2', if_not_exists=True)
				print('.', end='')
				q = (
					'CREATE TEMPORARY TABLE remainingFF AS '
					'SELECT b.id1, b.id2 FROM Fws a '
					'INNER JOIN FF b ON a.id2 = b.id1 '
					'INNER JOIN Fws c ON b.id2 = c.id2;'
					)
				db.fetch(query=q)
				print('.', end='')
				q = (
					'CREATE TEMPORARY TABLE countFF AS '
					'SELECT '
					'a.id2 AS id, '
					'CASE WHEN b.e < c.e THEN b.e ELSE c.e END AS deg '
					'FROM Fws a '
					'LEFT JOIN ('
					'SELECT id1, COUNT(*) AS e FROM remainingFF '
					'GROUP BY id1'
					') b ON a.id2 = b.id1 '
					'LEFT JOIN ('
					'SELECT id2, COUNT(*) AS e FROM remainingFF '
					'GROUP BY id2'
					') c ON a.id2 = c.id2 '
					'LEFT JOIN Collected d ON a.id2 = d.id '
					'WHERE d.id IS NULL '
					'ORDER BY deg DESC;'
					)
				db.fetch(query=q)
				print('.', end='')
				
				q = 'SELECT id, deg FROM countFF;'
				p = []
				for rows in db.fetchmany(batch=LIMIT, query=q):
					rows = set(rows)
					p.extend([0 if x[1] is None else x[1] for x in rows])
					tovisit.update(rows)
					if len(tovisit) > LIMIT:
						break
				print(f'\n\t{len(p)} - {min(p)} - {sum(p) / len(p)} - {max(p)}')
				
				q = f'SELECT id FROM countFF ORDER BY RANDOM() LIMIT {RAND};'
				for row in db.fetch(query=q):
					tovisit.add(row[0])
				
				db.drop('table', 'remainingFF')
				db.drop('table', 'countFF')
				db.drop('index', 'FF_id1')
				db.drop('index', 'FF_id2')

				if len(tovisit) == 0:
					break
			
			uid = tovisit.pop()
			apis[lookup_users][0].put([uid])
			data = apis[lookup_users][1].get()
			if data is None:
				continue
			
			obj = parse_user_object(data[0])
			print('\t', obj[0], obj[14], len(tovisit))
			
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
			
			rows[0].append(obj)
			rows[2].append((uid,))
			if len(rows[0]) > 100:
				print('\t\tinsert')
				db.fetch(name='insert_Users', params=rows[0])
				db.fetch(name='insert_FF', params=rows[1])
				db.fetch(name='insert_Collected', params=rows[2])
				rows = [[],[],[]]

		db.fetch(name='insert_Users', params=rows[0])
		db.fetch(name='insert_FF', params=rows[1])
		db.fetch(name='insert_Collected', params=rows[2])
	
	db.drop('index', 'Fws_id1')
	db.drop('index', 'Fws_id2')
	del db
	
	done.add(account)
	with open(f'{OUT}/done.txt', 'w') as f:
		for e in done:
			f.write(f'{e}\n')

# close threads
for func, v in apis.items():
	for _ in v[2]:
		v[0].put(None)
	for t in v[2]:
		t.join()
