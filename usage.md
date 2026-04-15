

## HTTP API

```bash
# write
curl -X PUT http://localhost:8080/key/{key} \
  -H "Content-Type: application/json" \
  -d '{"value": "..."}'

# read
curl http://localhost:8080/key/{key}

# delete
curl -X DELETE http://localhost:8080/key/{key}

# range query — returns all keys in [start, end] inclusive, sorted
curl "http://localhost:8080/range?start=a&end=z"
```

### Example responses

```bash
# PUT
curl -X PUT http://localhost:8080/key/city \
  -H "Content-Type: application/json" \
  -d '{"value": "bangalore"}'
# {"key":"city","value":"bangalore"}

# GET
curl http://localhost:8080/key/city
# {"key":"city","value":"bangalore"}

# RANGE
curl "http://localhost:8080/range?start=city&end=name"
# [{"key":"city","value":"bangalore"},{"key":"name","value":"alice"}]

# GET deleted key
curl http://localhost:8080/key/city
# {"error":"not found"}
```

---
## CLI (Interactive Shell)

BedrockDB provides an interactive CLI for local debugging and inspection of the storage engine.
```bash
go run cmd/cli/main.go
```

**Supported commands**
```
# basic operations
put <key> <value>
get <key>
delete <key>
range <start> <end>

# observability
stats
sstables

# exit
exit
```

**Example**
```
bedrockdb> put user:1 abhiram
OK

bedrockdb> get user:1
abhiram

bedrockdb> stats
memtable_size=3
immutable=false
sst_count=1

bedrockdb> sstables
[0] data/sst-000001.sst
```

---

## Run
```bash
# HTTP server
go run cmd/server/main.go

# CLI
go run cmd/cli/main.go

# docker
docker compose up --build
```