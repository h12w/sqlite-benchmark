sqlite-benchmark
================

Performance tests for bulk insert into SQLite memory database in Go.

```bash
$ go run main.go gen
$ go run main.go

.import csv: 3.811771501s
naive insert: 27.236900648s
prepare insert: 13.325263144s
tx insert: 23.319070431s
tx prepare insert: 10.93086166s
bulk insert: 7.019975066s
```
