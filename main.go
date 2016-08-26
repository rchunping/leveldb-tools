package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	goleveldb "github.com/rchunping/goleveldb/leveldb"
	goleveldbFilter "github.com/rchunping/goleveldb/leveldb/filter"
	goleveldbOpt "github.com/rchunping/goleveldb/leveldb/opt"
	"io"
	"log"
	"os"
	"time"
)

var (
	DB_PATH   string
	DUMP_FILE string
)

// iterator db, dump kv to file
func doExport() {

	opts := &goleveldbOpt.Options{}
	opts.OpenFilesCacher = goleveldbOpt.NoCacher
	opts.OpenFilesCacheCapacity = -1
	opts.ErrorIfMissing = true
	db, err := goleveldb.OpenFile(DB_PATH, opts)

	if err != nil {
		log.Fatalf("leveldb open error: %s", err.Error())
	}

	defer db.Close()

	// open dump file
	of, err := os.Create(DUMP_FILE)
	if err != nil {
		log.Fatalf("create export file error: %s", err.Error())
	}

	defer of.Close()

	bw := bufio.NewWriter(of)

	kvs := int64(0)

	ts := time.Now().UnixNano()
	ts0 := ts

	iter := db.NewIterator(nil, nil)

	ok := false
	ok = iter.First()

	for ok {

		key := iter.Key()
		value := iter.Value()

		bw.WriteString("KL:")
		kl := uint32(len(key))
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, kl)
		bw.Write(buf.Bytes())
		bw.Write(key)

		bw.WriteString("VL:")
		vl := uint32(len(value))
		buf = new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, vl)
		bw.Write(buf.Bytes())
		bw.Write(value)
		kvs += 1
		//log.Printf("%s : %s", key, value)
		ts2 := time.Now().UnixNano()

		if ts2-ts >= 1e9 {
			log.Printf("export %d kvs, %d/s", kvs, kvs/((ts2-ts0)/1e9))
			ts = ts2
		}

		ok = iter.Next()

	}
	iter.Release()
	if err := iter.Error(); err != nil {
		log.Fatalf("leveldb iterator error: %s", err.Error())
	}

	bw.Flush()

	log.Printf("export done, %d kvs.", kvs)
}

// IMPORTANT:
// scan each sst file, dump kv from the file
func doExport2() {

	opts := &goleveldbOpt.Options{}
	opts.OpenFilesCacher = goleveldbOpt.NoCacher
	opts.OpenFilesCacheCapacity = -1
	opts.ErrorIfMissing = true
	opts.RecoverDumpChan = make(chan goleveldbOpt.RecoverDumpData, 100)

	// open dump file
	of, err := os.Create(DUMP_FILE)
	if err != nil {
		log.Fatalf("create export file error: %s", err.Error())
	}

	defer of.Close()

	bw := bufio.NewWriter(of)

	kvs := int64(0)

	ts := time.Now().UnixNano()
	ts0 := ts
	tsSM := ts // 清理keySeqMap的计时器

	done := make(chan int, 1)
	type smt struct {
		seq uint64
		ts  int64
	}
	keySeqMap := make(map[string]smt, 0)

	go func() {

		for {
			kv, ok := <-opts.RecoverDumpChan
			if !ok {
				done <- 1
				break
			}

			key := kv.Key
			value := kv.Value

			if kv.KeyType != 1 {
				continue
			}

			skey := string(key)
			if sm, ok := keySeqMap[skey]; ok {
				// 更新的数据已经导出
				if sm.seq > kv.Seq {
					continue
				}
			}

			// NOTE: 因为del数据回跳过，seq只比较有value的数据，因此可能会有部分已删除数据重新变成有效

			//log.Printf("key: %#v %d %d %s %d %s", key, kv.Seq, kv.KeyType, string(key), len(value), string(value))
			// kvs++
			// if kvs > 100 {
			// 	os.Exit(0)
			// }
			// continue

			bw.WriteString("KL:")
			kl := uint32(len(key))
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, kl)
			bw.Write(buf.Bytes())
			bw.Write(key)

			bw.WriteString("VL:")
			vl := uint32(len(value))
			buf = new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, vl)
			bw.Write(buf.Bytes())
			bw.Write(value)
			kvs += 1
			//log.Printf("%s : %s", key, value)
			ts2 := time.Now().UnixNano()

			keySeqMap[skey] = smt{kv.Seq, ts2}

			//每分钟清理一次keySeqMap
			if ts2-tsSM > 60*1e9 {

				for k, sm := range keySeqMap {
					if ts2-sm.ts > 59*1e9 {
						delete(keySeqMap, k)
					}
				}

				tsSM = ts2
			}

			if ts2-ts >= 1e9 {
				log.Printf("export %d kvs, %d/s", kvs, kvs/((ts2-ts0)/1e9))
				ts = ts2
			}

		}

	}()

	db, err := goleveldb.RecoverFile(DB_PATH, opts)

	if err != nil {
		log.Fatalf("leveldb open error: %s", err.Error())
	}

	close(opts.RecoverDumpChan)

	defer db.Close()

	//等待结束
	<-done

	bw.Flush()

	log.Printf("export done, %d kvs.", kvs)
}

func doImport() {

	opts := &goleveldbOpt.Options{}
	opts.Filter = goleveldbFilter.NewBloomFilter(10)

	db, err := goleveldb.OpenFile(DB_PATH, opts)

	if err != nil {
		log.Fatalf("leveldb open error: %s", err.Error())
	}

	defer db.Close()

	// open dump file
	rf, err := os.Open(DUMP_FILE)
	if err != nil {
		log.Fatalf("open data file error: %s", err.Error())
	}

	defer rf.Close()

	br := bufio.NewReader(rf)

	kvs := int64(0)

	ts := time.Now().UnixNano()
	ts0 := ts

	var errx error
	for {
		errx = nil

		kl := make([]byte, 7)
		n, err := io.ReadFull(br, kl)

		if n != 7 {
			// re-read file
			// rf.Seek(0, 0)
			// br.Reset(rf)
			// continue
			if err != io.EOF {
				log.Printf("KL expact %d bytes got %d bytes", 7, n)
			}
			errx = err
			break
		}
		if string(kl[0:3]) != "KL:" {
			errx = errors.New("data file unknow KL format.")
			break
		}

		klb := bytes.NewBuffer(kl[3:])
		klen := uint32(0)
		err = binary.Read(klb, binary.BigEndian, &klen)
		if err != nil {
			errx = err
			break
		}

		key := make([]byte, klen)
		n, err = io.ReadFull(br, key)
		if n != int(klen) {
			log.Printf("key expact %d bytes got %d bytes", klen, n)
			errx = err
			break
		}

		vl := make([]byte, 7)
		n, err = io.ReadFull(br, vl)

		if n != 7 {
			log.Printf("VL expact %d bytes got %d bytes", 7, n)
			errx = err
			break
		}
		if string(vl[0:3]) != "VL:" {
			errx = errors.New("data file unknow VL format.")
			break
		}

		vlb := bytes.NewBuffer(vl[3:])
		vlen := uint32(0)
		err = binary.Read(vlb, binary.BigEndian, &vlen)
		if err != nil {
			errx = err
			break
		}

		value := make([]byte, vlen)
		n, err = io.ReadFull(br, value)
		if n != int(vlen) {
			log.Printf("value expact %d bytes got %d bytes", vlen, n)
			errx = err
			break
		}

		err = db.Put(key, value, nil)

		if err != nil {
			errx = err
			break
		}

		kvs += 1
		//log.Printf("%s : %s", key, value)
		ts2 := time.Now().UnixNano()

		if ts2-ts >= 1e9 {
			log.Printf("import %d kvs, %d/s", kvs, kvs/((ts2-ts0)/1e9))
			ts = ts2
		}

	}
	if errx != nil && errx != io.EOF {
		log.Panicf("import error: %s", errx.Error())
	}

	log.Printf("import done, %d kvs.", kvs)
}

func doRepair() {

	opts := &goleveldbOpt.Options{}
	opts.ErrorIfMissing = true
	opts.ForceRebuild = true            //强制重建所有sst/ldb文件
	opts.DropTableOnRebuildError = true // rebuild时遇到错误继续处理

	db, err := goleveldb.RecoverFile(DB_PATH, opts)

	if err != nil {
		log.Fatalf("leveldb repair error: %s", err.Error())
	}
	defer db.Close()

	log.Printf("leveldb repair ok.")
}

func main() {

	var dbpath, dumpfile, action *string

	dbpath = flag.String("dbpath", "", "database path")
	dumpfile = flag.String("file", "", "the dump data file")
	action = flag.String("action", "", "action:import/export/export2/repair")

	flag.Parse()

	if *dbpath == "" {
		log.Fatal("no database path.")
	}

	if !(*action == "import" || *action == "export" || *action == "export2" || *action == "repair") {
		log.Fatal("invalid action.")
	}

	DB_PATH = *dbpath
	if *action != "repair" && *action != "repair2" {
		if *dumpfile == "" {
			log.Fatal("no dump file.")
		}
		DUMP_FILE = *dumpfile
	}

	if *action == "export" {
		doExport()
	} else if *action == "export2" {
		doExport2()
	} else if *action == "import" {
		doImport()
	} else if *action == "repair" {
		doRepair()
	}

}
