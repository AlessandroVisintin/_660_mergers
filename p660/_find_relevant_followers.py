from p660.mergers import MERGERS
from StorageUtils.SQLite import SQLite

import math


def norm(x, mean, sigma):
	
	C = 1 / (sigma * math.sqrt(2 * math.pi))
	return C * math.exp(-(x - mean)**2 / (2 * sigma**2))

OUT = 'out/p660'
CFG = 'config/p660'

fws = SQLite(f'{OUT}/fws.db')
fws.add_function('norm', 3, norm)

clusters = list(MERGERS.keys())
#clusters = ['adobe','activision']

fws.fetch(query=(
	'CREATE TABLE IF NOT EXISTS redundancy('
	'id INTEGER, table TEXT, PRIMARY KEY(id,table)) WITHOUT ROWID;'
	))


q = (
		'INSERT OR IGNORE INTO redundancy(id,redundancy) '
		'SELECT id, {j}_{k} FROM {j}_{k}_best WHERE true;'
		)
for i,j in enumerate(clusters[:-1]):
	for k in clusters[i+1:]:
		print(j,k)
		
		num = len(MERGERS[j]['cluster']) + len(MERGERS[k]['cluster'])
		
		fws.fetch(query=q.format(j=j, k=k))
		
