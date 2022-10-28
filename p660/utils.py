import time
from datetime import datetime as dt


def twt2stamp(twitter_date:str) -> int:
	"""
	
	Convert Twitter time string to timestamp.
	
	Args:
		twitter_date : twitter date to convert.
	
	Returns:
		timestamp value.
	
	"""
	
	t = dt.strptime(twitter_date, '%a %b %d %H:%M:%S %z %Y')
	return int(round(t.timestamp()))


def crs2stamp(cursor:int) -> int:
	"""
	
	Convert Twitter cursor to timestamp.
	
	Args:
		cursor : Twitter cursor to convert.
	
	Returns:
		timestamp value.
	
	"""

	return round((cursor >> 22) / 250, 3)
    

def stamp2crs(stamp:int) -> int:
	"""
	
	Convert timestamp to Twitter cursor.
	
	Args:
		stamp : timestamp to convert.
	
	Returns:
		Twitter cursor value.
	
	"""
	
	return int(round((stamp * 250))) << 22


def parse_user_object(obj:dict) -> tuple:
	"""
	
	Parse Twitter user object.
	
	"""

	key_mapping = [
		('id',int),
		('created_at',twt2stamp),
		('default_profile',bool),
		('default_profile_image',bool),
		('description',str),
		('favourites_count',int),
		('followers_count',int),
		('friends_count',int),
		('listed_count',int),
		('location',str),
		('name',str),
		('profile_banner_url',str),
		('profile_image_url_https',str),
		('protected',bool),
		('screen_name',str),
		('statuses_count',int),
		('url',str),
		('verified',bool),
	]
	
	out = [f(obj[k]) if k in obj else None for k,f in key_mapping]
	out.append(int(time.time()))
	return out
