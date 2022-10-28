from p660.mergers import MERGERS
from StorageUtils.SQLite import SQLite


OUT = 'out/p660'
CFG = 'config/p660'

p660 = SQLite(f'{OUT}/p660.db', config=f'{CFG}/p660.yaml')
fws = SQLite(f'{OUT}/fws.db')

ids = {}
for k,v in MERGERS.items():
	print(f'Loading ids - {k}')
	t = '","'.join(str(x) for x in v['cluster'])
	q = 'SELECT * FROM Users WHERE username IN ("{t}");'.format(t=t)
	ids[k] = [x[0] for x in p660.yields(query=q)]
	

for k,v in ids.items():
	print(k)
	
	fws.fetch(query=(
		f'CREATE TABLE IF NOT EXISTS {k}_fws('
		f'id INTEGER, PRIMARY KEY(id)) WITHOUT ROWID;'
		))
	
	t = '","'.join(str(x) for x in v)
	q1 = 'SELECT DISTINCT Users_id2 FROM IsFollowedBy WHERE Users_id1 IN ("{t}");'.format(t=t)
	q2 = f'INSERT OR REPLACE INTO {k}_fws (id) VALUES (?);'
	cache = []
	for i,row in enumerate(p660.yields(query=q1)):
		if not i % 10000: print(i, end=' ')
		cache.append(row)
		if len(cache) > 10000:
			fws.fetch(query=q2, params=cache)
			cache = []
	print('\n')
	fws.fetch(query=q2, params=cache)
	
	fws.fetch(query=(
		f'CREATE TABLE IF NOT EXISTS {k}_ffws('
		f'id1 INTEGER, id2 INTEGER, PRIMARY KEY(id1,id2)) WITHOUT ROWID;'
		))
	q1 = ('SELECT a.Users_id1, a.Users_id2 FROM IsFollowedBy AS a INNER JOIN ('
		'SELECT * FROM IsFollowedBy WHERE Users_id1 IN ("{t}")'
		') b ON a.Users_id1 = b.Users_id2;'
		).format(t=t)
	q2 = f'INSERT OR REPLACE INTO {k}_ffws (id1,id2) VALUES (?,?);'
	cache = []
	for i,row in enumerate(p660.yields(query=q1)):
		if not i % 5000: print(i, end=' ')
		cache.append(row)
		if len(cache) > 10000:
			fws.fetch(query=q2, params=cache)
			cache = []
	print('\n')
	fws.fetch(query=q2, params=cache)
	