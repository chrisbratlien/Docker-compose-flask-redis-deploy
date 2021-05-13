# chiametry

## inspiration

https://github.com/kartheekgottipati/Docker-compose-flask-redis-deploy

https://runnable.com/docker/python/docker-compose-with-flask-apps

# INSTALL

```
cp .env.sample .env
```

Edit PLOT_LOGS_ROOT in .env

# start container

```
$ docker-compose up
```

# ingest

1. Untar all of the tarballs in /logs

2. connect interactively to running container

```
docker exec -it <container> /bin/sh
```

3. run ingestion script

```
usage: ingest_plot_logs.py [-h] [--flushall-before-ingest] [--small-sample]

optional arguments:
  -h, --help            show this help message and exit
  --flushall-before-ingest
                        flush Redis DB
  --small-sample        only ingest a few random plots (for debugging)
```

```
python ingest_plot_logs.py
```

Example output:

```
ingesting plot cd57b9001eb70aa5295d34eefddca0010cd478e48cd936ccde746afe9328bfb7 (/logs/tractor10/lotus/logs/16485.log)
ingesting plot 205786edcbadebbf2ef7dc791a5650eb1f4550d374a8888d384789e65e217576 (/logs/tractor10/lotus/logs/16520.log)
ingesting plot b7d294b782c17b9a09a8876f06d8367ea32b3c6ef7972e58028c092327001163 (/logs/tractor10/lotus/logs/16523.log)
ingesting plot 547bef5aa3ce9f5806bfe67849efc7a625949ea860261dc50c8bda43a018fdd8 (/logs/tractor10/lotus/logs/16586.log)
ingesting plot 8fe98cac920e13e2af6e7ee5b132b9f7dc375d951c6bc26faf040bb349dadf6b (/logs/tractor10/lotus/logs/16679.log)
```

Safe to reingest, it will skip recognized plot ids.

```
already have it. skipping plot: cd57b9001eb70aa5295d34eefddca0010cd478e48cd936ccde746afe9328bfb7
already have it. skipping plot: 205786edcbadebbf2ef7dc791a5650eb1f4550d374a8888d384789e65e217576
already have it. skipping plot: b7d294b782c17b9a09a8876f06d8367ea32b3c6ef7972e58028c092327001163
already have it. skipping plot: 547bef5aa3ce9f5806bfe67849efc7a625949ea860261dc50c8bda43a018fdd8
already have it. skipping plot: 8fe98cac920e13e2af6e7ee5b132b9f7dc375d951c6bc26faf040bb349dadf6b
```

## (optional) flush redis before ingest

python ingest_plot_logs.py --flushall-before-ingest

## (optional) small random sample

python ingest_plot_logs.py --flushall-before-ingest --small-sample

# Endpoints

[http://localhost:5000/api/plot_ids](http://localhost:5000/api/plot_ids)

[http://localhost:5000/api/tractor_ids](http://localhost:5000/api/tractor_ids)

[http://localhost:5000/api/plots_by_total_time](http://localhost:5000/api/plots_by_total_time)

# MISC NOTES

r.lpush()

from redis import Redis
redis = Redis(host='redis', port=6379, charset="utf-8", decode_responses=True)

> > > redis.get('plot_count')
> > > '100'
> > > redis.hmget('plot:8fe98cac920e13e2af6e7ee5b132b9f7dc375d951c6bc26faf040bb349dadf6b','plot_id')
> > > ['8fe98cac920e13e2af6e7ee5b132b9f7dc375d951c6bc26faf040bb349dadf6b']
> > > redis.hmget('plot:8fe98cac920e13e2af6e7ee5b132b9f7dc375d951c6bc26faf040bb349dadf6b','copy_time')
> > > ['891.216']
> > > redis.hmget('plot:8fe98cac920e13e2af6e7ee5b132b9f7dc375d951c6bc26faf040bb349dadf6b','copy_time')

# remove everything from Redis

redis.flushall()
