from StorageUtils.SQLite import SQLite


OUT = 'out/p660'
CFG = 'config/p660'

db = SQLite(f'{OUT}/p660.db',config=f'{CFG}/p660.yaml')

usernames = []
with open(f'{OUT}/usernames.txt', 'r') as f:	 
	unm = '","'.join([line.strip() for line in f])

query = f'SELECT username,id FROM Users WHERE username IN ("{unm}");'
ref = {b:[a,0] for a,b in db.fetch(query=query)}

for i,row in enumerate(db.yields(query='SELECT * FROM IsFollowedBy;')):
	if not i % 50000: print(i)
	if row[0] in ref:
		query = f'SELECT EXISTS (SELECT 1 FROM IsFollowedBy WHERE Users_id1 = {row[1]})'
		ref[row[0]][1] += db.fetch(query=query)[0][0] 

	

# 	try:
# 		if row[1] in ref[row[0]][0] and row[1] in fws: 
# 		ref[row[0]][0].add(row[1])
# 		 
# 	if row[0] in ref:
# 		
# 		pass
# 	else:
# 		fws.add(row[0])

# 	
# 	query = f'SELECT Users_id2 FROM IsFollowedBy WHERE Users_id1 = "{aid}";'
# 	
# 		


# query = f'SELECT Users_id2 FROM IsFollowedBy WHERE Users_id1 = "{userid}"'
# db.add_query('select', 'tmp', query)
# fws = {row[0]:0 for row in db.select('tmp')}

# try:
# 	query = 'SELECT * FROM IsFollowedBy'
# 	db.add_query('select', 'tmp', query)
# 	count = 0
# 	for row in db.select('tmp'):
# 		count += 1
# 		if row[0] in fws and row[1] in fws:
# 			fws[row[0]] += 1
# except:
# 	pass

# fws = sorted([(k,v) for k,v in fws.items()], key=lambda t:t[1], reverse=True)[:1000]
# for uid,num in fws:
# 	query = f'SELECT username FROM Users WHERE id = "{uid}"'
# 	db.add_query('select', 'tmp', query)
# 	username = [row for row in db.select('tmp')][0][0]
# 	print(username, num, end=' ')
	