SCHEMA.md

plot_ids

a list
idx = redis.rpush('list::plot_ids','blahblahlonghash')

0 (long hash "ID" found in plot log)
1 (long hash "ID" found in plot log)

etc

now I can refer to this plot by its smaller index.
