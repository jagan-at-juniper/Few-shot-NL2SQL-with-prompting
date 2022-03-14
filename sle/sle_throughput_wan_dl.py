
# site_key = "SleApWirelink_wirelink_stats.e3af660b-d724-4063-b378-a180a58efe7c.*"
#
# site_key = "SleApWirelink_wirelink_stats.51db4ed9-6351-438c-810a-83fd3b98fb7f.2021-10-19.02"

day = "2022-01-19"

import redis
r = redis.Redis(host="redis-proxy-marvis-production.mist.pvt", port=6379)

def get_wan_dl(site, day):
    redis_key = "SleApWirelink_wirelink_stats.{site}.agg".format(site=site)
    res = r.get(redis_key)
    print(res)

    redis_key = "SleApWirelink_wirelink_stats.{site}.hist_agg".format(site=site)
    res = r.get(redis_key)
    print(res)

    for hr in range(0, 23):
        if hr < 10:
            hour = "0{}".format(hr)
        else:
            hour = "{}".format(hr)
        redis_key = "SleApWirelink_wirelink_stats.{site}.{day}.{hour}".format(site=site, day=day, hour=hour)

        res = r.get(redis_key)
        print(day, hour, res)

LS1099 = "51db4ed9-6351-438c-810a-83fd3b98fb7f"
get_wan_dl(LS1099, "2021-11-01")
get_wan_dl(LS1099, "2021-11-02")

get_wan_dl(LS1099, "2021-10-18")
get_wan_dl(LS1099, "2021-10-19")
get_wan_dl(LS1099, "2021-10-20")


Walmart_3123 = "e3af660b-d724-4063-b378-a180a58efe7c"
get_wan_dl(Walmart_3123, "2021-10-18")
get_wan_dl(Walmart_3123, "2021-10-19")
get_wan_dl(Walmart_3123, "2021-10-20")

Walmart_4132 = '92a8ddf2-1d58-4a62-908d-370685996177'
get_wan_dl(Walmart_4132, "2021-11-01")
get_wan_dl(Walmart_4132, "2021-11-02")
