from p660.utils import parse_user_object
from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import lookup_users
from JSONWrap.utils import load

from queue import Queue, PriorityQueue
from threading import Thread


OUT = 'out/p660'
CFG = 'config/p660'
CL_CFG = f'{CFG}/clusters.yaml'
CR_CFG = 'config/WebUtils/twitterapi_cred.yaml'
OLD_OUT = f'{OUT}/_p660.db'
P660_OUT = f'{OUT}/p660.db'
P660_CFG = f'{CFG}/p660.yaml'

apis = {
	lookup_users: [Queue(), Queue(), []],
	}
for name, k in load(CR_CFG).items():
	for func, v in apis.items():
		t = Thread(target=func, args=(k, 'user', v[0], v[1]))
		t.start()
		v[2].append(t)


old = SQLite(OLD_OUT)
p660 = SQLite(P660_OUT, config=P660_CFG)

mergers = {}
for j,u in load(CL_CFG).items():
	for account in u[0]:
		print(account)
		
		p660.fetch(name='create_fws', format={'t':account})
		
		apis[lookup_users][0].put([account])
		data = apis[lookup_users][1].get()
		obj = parse_user_object(data[0])
		
		p660.fetch(name='insert_Users', params=[obj])
		
		q = f'SELECT * FROM IsFollowedBy WHERE Users_id1 = {obj[0]};'
		cache = []
		for row in old.yields(query=q):
			cache.append((row[0],row[1],None,None))
			if len(cache) > 10000:
				p660.fetch(name='insert_fws', format={'t':account}, params=cache)
				cache = []
		p660.fetch(name='insert_fws', format={'t':account}, params=cache)
		cache = []
		
del old
del p660
