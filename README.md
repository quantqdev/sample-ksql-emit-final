### Run

```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088 # Then key in content of query.ksql
node index.js
```

### Result

If one window has no event then result of previous window is not triggered even though it has many events.