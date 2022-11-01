"""

Perform a simple request.

"""

from WebUtils.threaded_twitter import apiv11
from JSONWrap.utils import load


# load authentication	
credentials = load('config/WebUtils/twitterapi_cred.yaml')

# account
USERNAME = 'twitter'

# get API
key = list(credentials.keys())[0]
api = apiv11(credentials[key], 'user')

# collect
#d1 = api.get_followers(screen_name=USERNAME)
#d2 = api.get_follower_ids(screen_name=USERNAME)

d3 = api.lookup_users(user_id=[2981461])

#rates = api.rate_limit_status()

#d3 = api.get_friendship(source_screen_name='tinexta', target_screen_name='sergio_dionese')
