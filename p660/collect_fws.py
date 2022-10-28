# from p660.utils import parse_user_object
# from StorageUtils.SQLite import SQLite
# from WebUtils.threaded_twitter import get_follower_ids
# from WebUtils.threaded_twitter import get_followers
# from WebUtils.threaded_twitter import lookup_users
# from JSONWrap.utils import load

# from pathlib import Path
# from queue import Queue
# from threading import Thread


# OUT = 'out/p660'
# CFG = 'config/p660'

# p660 = SQLite(f'{OUT}/p660.db', config=f'{CFG}/p660.yaml')
# clusters = load(f'{CFG}/clusters.yaml')
# credentials = load('config/WebUtils/twitterapi_cred.yaml')

# apis = {
# 	get_friend_ids : [Queue(), Queue(), []],
# 	get_friends: [Queue(), Queue(), []],
# 	get_followers : [Queue(), Queue(), []],
# 	get_follower_ids: [Queue(), Queue(), []],
# 	lookup_users: [Queue(), Queue(), []],
# 	}

# for name, k in credentials.items():
# 	for func, v in apis.items():
# 		t = Thread(target=func, args=(k, 'user', v[0], v[1]))
# 		t.start()
# 		v[2].append(t)

# mergers = {}
# for j,u in clusters.items():
# 	for v in u[1]:
# 		try:
# 			mergers[v].update(set(u[0]))
# 		except KeyError:
# 			mergers[v] = set(u[0])


# # list of users to collect
# USERS = ['twitter','verified']
# OUTPATH = 'out/WebUtils/threaded_twitter'

# # load authentication	
# credentials = load('config/WebUtils/twitterapi_cred.yaml')

# # create threads for accounts < 200 followers and < 5000 followers
# end = Event()
# in_200 = Queue()
# in_5000 = Queue()
# out_200 = Queue()
# out_5000 = Queue()

# apis = {
# 	
# 	200 : [
# 		Thread(
# 			target=get_followers,
# 			args=(v, 'user', end, in_200, out_200)
# 			) for k,v in credentials.items()
# 		],
# 	
# 	5000 : [
# 		Thread(
# 			target=get_followers_ids,
# 			args=(v, 'user', end, in_5000, out_5000)
# 			) for k,v in credentials.items()
# 		]
# 	
# 	}

# # start threads
# for t in apis[200]:
# 	t.start()
# for t in apis[5000]:
# 	t.start()

# # create output folder 
# outpath = Path(f'{OUTPATH}')
# outpath.mkdir(parents=True, exist_ok=True)

# for USERNAME in USERS:
# 	# collect
# 	with (outpath / USERNAME).open('w') as f:
# 		print(USERNAME)
# 		count = 0
# 		cursor = -1
# 		while not cursor == 0:
# 			print(count, cursor, end=', ')
# 			in_200.put((USERNAME, cursor, 200))
# 			data = out_200.get()
# 			for fws in data['users']:
# 				userid = fws['id']
# 				followers = fws['followers_count']
# 				protected = fws['protected']
# 				f.write(f'{userid}\t{followers}\t{protected}\n')
# 				count += 1
# 			cursor = data['next_cursor']
# 			
# 			in_5000.put((USERNAME, cursor, 5000))
# 			data = out_5000.get()
# 			for fws in data['ids']:
# 				f.write(f'{fws}\n')
# 				count += 1
# 			cursor = data['next_cursor']
# 	
# 			# write log
# 			with (outpath / 'log.txt').open('w') as g:
# 				g.write(f'{USERNAME}\t{cursor}\n')
# 			f.flush()
# 		print('')

# # close threads
# end.set()
# for t in apis[200]:
# 	in_200.put(None)
# 	t.join()
# for t in apis[5000]:
# 	in_5000.put(None)
# 	t.join()
