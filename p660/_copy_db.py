from StorageUtils.SQLite import SQLite


OUT = 'out/p660'
CFG = 'config/p660'

db = SQLite(f'{OUT}/p660.db',config=f'{CFG}/p660.yaml')
out = SQLite(f'{OUT}/Users.db',config=f'{CFG}/p660.yaml')

with open(f'{OUT}/usernames.txt', 'r') as f:	 
	unm = '","'.join([line.strip() for line in f])
	
query = f'SELECT * FROM Users WHERE username IN ("{unm}");'
ref = db.fetch(query=query)
out.fetch(name='insert_Users', params=ref)

cache = []
for i,row in enumerate(db.yields(query=f'SELECT * FROM IsFollowedBy;')):
	if not i % 10000: print(i)
	for e in ref:
		if e[0] == row[0]:
			cache.append(row)
			break
	
	if len(cache) > 5000:
		out.fetch(name='insert_IsFollowedBy', params=cache)
		cache = []

out.fetch(name='insert_IsFollowedBy', params=cache)

del db
del out
