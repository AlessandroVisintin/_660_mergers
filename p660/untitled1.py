from StorageUtils.SQLite import SQLite
from JSONWrap.utils import load


OUT = 'out/p660'
CFG = 'config/p660'
DB_OUT = f'{OUT}/p660.db'
DB_CFG = f'{CFG}/p660.yaml'
CL_CFG = f'{CFG}/clusters.yaml'

mergers = {}
for j,u in load(CL_CFG).items():
	for v in u[1]:
		try:
			mergers[v].update(set(u[0]))
		except KeyError:
			mergers[v] = set(u[0])

p660 = SQLite(DB_OUT, config=DB_CFG)

for merger,accounts in mergers.items():
	print(merger)
	for account in accounts:
		print(account)
		
		p660.drop('index', f'{account}_Fwsid1', if_exists=True)
		p660.drop('index', f'{account}_Fwsid2', if_exists=True)
		p660.drop('index', f'{account}_FFid1', if_exists=True)
		p660.drop('index', f'{account}_FFid2', if_exists=True)
