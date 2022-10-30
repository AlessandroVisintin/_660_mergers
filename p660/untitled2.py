from StorageUtils.SQLite import SQLite

OUT = 'out/p660'
CFG = 'config/p660'

p660 = SQLite(f'{OUT}/p660.db', config=f'{CFG}/p660.yaml')

p660.schema()

account = 'ATVI_AB'

p660.fetch(name='create_collected', format={'t':account})

print(p660.fetch(query=f'SELECT COUNT(*) FROM {account}_FF WHERE id2 = 844285817266745344'))


print(p660.size(f'{account}_Fws'))
print('\n')

q = (
	  f'CREATE TEMPORARY TABLE {account}_remainingFws AS '
	  f'SELECT a.id2 AS id FROM {account}_Fws a '
	  f'LEFT JOIN {account}_collected b ON a.id2 = b.id '
	  f'WHERE b.id IS NULL;'
	  )
p660.fetch(query=q)

print(p660.size(f'{account}_remainingFws'))
for i,row in enumerate(p660.yields(query=f'SELECT * FROM {account}_remainingFws')):
	if i < 10:
		print(row)
		continue
	break
print('\n')

q = (
	f'CREATE TEMPORARY TABLE {account}_remainingFF AS '
	f'SELECT b.id1, b.id2 FROM {account}_remainingFws a '
	f'INNER JOIN {account}_FF b ON a.id = b.id1 '
	f'INNER JOIN {account}_remainingFws c ON b.id2 = c.id;'
	)
p660.fetch(query=q)

print(p660.size(f'{account}_remainingFF'))
for i,row in enumerate(p660.yields(query=f'SELECT * FROM {account}_remainingFF')):
	if i < 10:
		print(row)
		continue
	break
print('\n')

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
	f'LIMIT 100;'
	)
for row in p660.yields(query=q):
	print(row)


# for row in db.yields(query='SELECT * FROM Users'):
#  	print(row)


# print(db.size('Collected'))
# for row in db.yields(query='SELECT * FROM Collected;'):
#  	print(row)
# 	break

# q = (
# 	f'SELECT * FROM ATVI_AB_Fws a '
# 	f'LEFT JOIN '
# 	f'(SELECT * FROM ATVI_AB_FF) b ON a.id2 = b.id1 '
# 	f'WHERE b.id1 IS NULL '
# 	f'LIMIT 0,1000;'
# 	)

# out = db.fetch(query=q)

# o1 = db.fetch(query='SELECT * FROM ATVI_AB_FF')
# o2 = db.fetch(query='SELECT * FROM ATVI_AB_Fws')