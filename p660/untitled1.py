from p660.utils import parse_user_object, twt2stamp
from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import lookup_users
from JSONWrap.utils import load

import time
from queue import Queue
from threading import Thread


OUT = 'out/p660'
CFG = 'config/p660'

old = SQLite(f'{OUT}/_p660.db')
db = SQLite(f'{OUT}/p660.db', config=f'{CFG}/p660.yaml')
credentials = load('config/WebUtils/twitterapi_cred.yaml')
clusters = load(f'{CFG}/clusters.yaml')

in_look = Queue()
out_look = Queue()

apis = [
 	Thread(
		target=lookup_users,
		args=(v, 'user', in_look, out_look)
		) for k,v in credentials.items()]

for t in apis:
 	t.start()

for k,v in clusters.items():
	for username in v[0]:
		print(username)
		
		in_look.put([username])
		data = out_look.get()
		
		row = parse_user_object(data[0])
		db.fetch(name='insert_Users', params=[row])
		
		f = {'t':username}
		db.fetch(name='create_fws', format=f)
		
		q = f'SELECT * FROM IsFollowedBy WHERE Users_id1 = {data[0]["id"]}'
		cache = []
		for i,row in enumerate(old.yields(query=q)):
			if not i%1000: print(i, end=' ')
			cache.append((row[0],row[1],None,None))
			if len(cache) > 10000:
				db.fetch(name='insert_fws', format=f, params=cache)
				cache = []
		print('\n')
		db.fetch(name='insert_fws', format=f, params=cache)


for _ in apis:
	in_look.put(None)
for t in apis:
	t.join()

del old
del db
