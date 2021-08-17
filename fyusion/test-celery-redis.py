import redis
r = redis.Redis(host='10.181.9.103', port=6379, db=0)
r.rpush('hispanic', 'uno')
r.rpush('hispanic', 'dos')
r.rpush('hispanic', 'tres')
r.rpush('hispanic', 'cuatro')

x = r.llen('hispanic')
print(x)




import redis
r = redis.Redis(host='10.181.9.103', port=6379, db=0)

r.set("hispanic", "Hello Redis!!!")
msg = r.get("hispanic")
print(msg)

x = r.llen('hispanic')
print(x)


==========================================================================

import redis
r = redis.Redis(host='10.181.9.103', port=6379, db=0)

r.set("images", "https://storage.cloud.google.com/sushant-julo-test-blucket/images/image1.png")
msg = r.get("images")
print(msg)


========================================================================

import redis

r = redis.Redis(host='10.200.139.15', port=6379, db=0)

r.set('foo', 'bar')
value = r.get('foo')
print(value)
