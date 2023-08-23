### Run

```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
# Key in content of query.ksql

node index.js
```

### Result

If one window has no event then result of previous window is triggered even thought it has many events.