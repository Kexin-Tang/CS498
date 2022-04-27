import redis

pool = redis.ConnectionPool(
    host='localhost',
    port=6379,
    decode_responses=True
)
r = redis.Redis(connection_pool=pool)

cityCode = {
    "LA": 1,
    "SD": 2,
    "Portland": 3,
    "Salem": 4
}