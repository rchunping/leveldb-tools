Install
---


go get github.com/rchunping/leveldb-tools



Usage
---

## DO BACKUP YOUR DATA FIRST!!!

#### repair db
```sh
leveldb-tools -dbpath /path/to/leveldb -action repair
```
this action will rebuild each .ldb file.

----

#### import db
```sh
leveldb-tools -dbpath /path/to/leveldb -action import -file /path/to/dump.file
```

----

#### export db
```sh
leveldb-tools -dbpath /path/to/leveldb -action export -file /path/to/dump.file
```

iterator through the whole db, and dump all the key/value pairs to file.

----

#### export db(force)
```sh
leveldb-tools -dbpath /path/to/leveldb -action export2 -file /path/to/dump.file
```
try this action if action=export not work.

this will scan all .ldb file,and dump key/value from each .ldb file.

But there are some shortcomings:
```
1. Not 100% of your data will come back,but at least most of them.
2. If you update some key frequently, there may be multiple values under the same key in Level-0 tabel file.
   I try to use the latest value.
3. because of (2), some deleted keys may be "come back".
```
