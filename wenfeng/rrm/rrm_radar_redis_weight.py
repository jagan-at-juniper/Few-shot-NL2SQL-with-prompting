
# site_key = "SleApWirelink_wirelink_stats.e3af660b-d724-4063-b378-a180a58efe7c.*"
#
# site_key = "SleApWirelink_wirelink_stats.51db4ed9-6351-438c-810a-83fd3b98fb7f.2021-10-19.02"

day = "2021-10-19"

import redis
redis_conn = redis.Redis(host="redis-proxy-marvis-production.mist.pvt", port=6379)


# redis_key = "rrm/policy/site/{}/channel/weights".format(site_id)

def test_get_channel_weight():
    scope = "*"
    # match_pattern = "rrm/policy/{scope}/*".format(scope=scope)
    match_pattern = "rrm/policy/site/*/channel/weights"
    for k in redis_conn.scan_iter(match=match_pattern):
        val_json = redis_conn.hgetall(k)
        # print(k, val_json)
        # if val_json:
        print(k, val_json)



