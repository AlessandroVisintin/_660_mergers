from p660.mergers import MERGERS
from StorageUtils.SQLite import SQLite

import math
import matplotlib.pyplot as plt

OUT = 'out/p660'
CFG = 'config/p660'

fws = SQLite(f'{OUT}/fws.db')
clusters = list(MERGERS.keys())

fws.add_function('pow', 2, math.pow)

q = (
	  'SELECT '
	  'a.id, a.deg1, a.deg2, b.redundancy, CASE '
	  'WHEN a.deg1 < a.deg2 THEN a.deg1 / pow(2,b.redundancy) '
	  'ELSE a.deg2 / pow(2,b.redundancy) END AS relevance '
	  'FROM {j}_{k}_best a '
	  'INNER JOIN redundancy b ON a.id = b.id '
	  'ORDER BY relevance DESC;'
	  )

for i,j in enumerate(clusters[:-1]):
	
# 	plots = [[],[]]	
# 	check = set(y for x in MERGERS[j]['mergers'] for y in x.split('_'))

	for k in clusters[i+1:]:

		out = fws.fetch(query=q.format(j=j, k=k))
		
		break
	break


# 		if k in check:
# 			plots[0].append([])
# 			for row in fws.yields(query=q.format(a=j, b=k)):
# 				if row[1] < row[2]:
# 					plots[0][-1].append(row[1])
# 				else:
# 					plots[0][-1].append(row[2])
# 		else:
# 			plots[1].append([])
# 			for row in fws.yields(query=q.format(a=j, b=k)):
# 				if row[1] < row[2]:
# 					plots[1][-1].append(row[1])
# 				else:
# 					plots[1][-1].append(row[2])
# 	
# 	
# 	ax = plt.gca()
# 	ax.set_xlim([0, 2500])

# 	for y in plots[0]:
# 		plt.plot(range(len(y)), y, c='red')

# 	for y in plots[1]:
# 		plt.plot(range(len(y)), y, c=(0,0,1,0.2))

# 	plt.show()

del fws
