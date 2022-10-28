from p660.mergers import MERGERS

from StorageUtils.SQLite import SQLite


OUT = 'out/p660'
CFG = 'config/p660'

fws = SQLite(f'{OUT}/fws.db')

#tenderer, tenderee
clusters = list(MERGERS.keys())
#clusters = ['adobe','activision']

for i,j in enumerate(clusters[:-1]):
	for k in clusters[i+1:]:
		print(j,k)
		fws.fetch(query=(
			f'CREATE TABLE IF NOT EXISTS {j}_{k}_best('
			'id INTEGER, deg1 INTEGER, deg2 INTEGER, PRIMARY KEY(id)) WITHOUT ROWID;'
			))
		
		print('\tTemp deg1')
		q = (
			f'CREATE TEMPORARY TABLE deg1 AS '
			f'SELECT a.id1, COUNT(*) AS deg1 FROM {j}_ffws AS a '
			f'INNER JOIN ({k}_fws) b ON a.id2 = b.id GROUP BY a.id1;'
		)
		fws.fetch(query=q)
		
		print('\tTemp deg2')
		q = (
			f'CREATE TEMPORARY TABLE deg2 AS '
			f'SELECT a.id1, COUNT(*) AS deg2 FROM {k}_ffws AS a '
			f'INNER JOIN ({j}_fws) b ON a.id2 = b.id GROUP BY a.id1;'
			)
		fws.fetch(query=q)
		
		print('\tOuter join')
		q = (
			'SELECT id1, deg1.deg1, deg2.deg2 FROM deg1 '
			'LEFT JOIN deg2 USING(id1) '
			'UNION ALL '
			'SELECT id1, deg1.deg1, deg2.deg2 FROM deg2 '
			'LEFT JOIN deg1 USING(id1) '
			'WHERE deg1.deg1 IS NULL'
			)
		cache = []
		q1 = f'INSERT OR REPLACE INTO {j}_{k}_best (id,deg1,deg2) VALUES (?,?,?);'
		for i,row in enumerate(fws.yields(query=q)):
			if not i % 1000: print(i, end=' ')
			cache.append(tuple(0 if x is None else x for x in row))
			if len(cache) > 10000:
				fws.fetch(query=q1, params=cache)
				cache = []
		print('\n')
		
		fws.fetch(query=q1, params=cache)
		
		fws.fetch(query='DROP TABLE deg1;')
		fws.fetch(query='DROP TABLE deg2;')
	
	
		#print('\tUnion')
		#q = f'SELECT COUNT(*) FROM (SELECT * FROM {j}_fws UNION SELECT * FROM {k}_fws);'
		#jk_u = fws.fetch(query=q)[0]

# 		q = (
# 			f'SELECT COUNT(*) FROM ( '
# 			f'SELECT a.id FROM {j}_fws AS a INNER JOIN ({k}_fws) b ON a.id = b.id '
# 			f'UNION '
# 			f'SELECT a.id2 FROM {j}_ffws AS a INNER JOIN ({k}_fws) b ON a.id2 = b.id '
# 			f'UNION '
# 			f'SELECT b.id2 FROM {j}_fws AS a INNER JOIN ({k}_ffws) b ON a.id = b.id2 '
# 			');'
# 			)
# 		jk_i = fws.fetch(query=q)[0]


del fws


# for i,row in enumerate(db.yields(query='SELECT * FROM IsFollowedBy;')):
# 	if not i % 50000: print(i)
# 	if row[0] in ref:
# 		query = f'SELECT EXISTS (SELECT 1 FROM IsFollowedBy WHERE Users_id1 = {row[1]})'
# 		ref[row[0]][1] += db.fetch(query=query)[0][0] 
