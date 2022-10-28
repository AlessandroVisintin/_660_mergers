from StorageUtils.SQLite import SQLite

OUT = 'out/p660'

db = SQLite(f'{OUT}/p660.db')

for row in db.yields(query='SELECT * FROM Users'):
 	print(row)

db.schema()
print(db.size('atvi_ab_Fws'))
for row in db.yields(query='SELECT * FROM atvi_ab_Fws;'):
 	print(row)
 	break

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