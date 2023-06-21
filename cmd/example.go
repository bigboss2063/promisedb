package main

import (
	"bytes"
	"github.com/bigboss2063/promisedb"
	"os"
	"time"
)

func main() {
	option := promisedb.DefaultOption()
	db, err := promisedb.OpenDB(option)
	defer db.Close()
	if err != nil {
		panic(err.Error())
	}

	err = db.Put([]byte("hello"), []byte("world"))
	if err != nil {
		panic(err.Error())
	}

	et, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err.Error())
	}

	if bytes.Compare(et.Value, []byte("world")) != 0 {
		panic(et.Value)
	}

	err = db.Del([]byte("hello"))
	if err != nil {
		panic(err.Error())
	}

	et, err = db.Get([]byte("hello"))
	if err != promisedb.ErrKeyNotExist {
		panic(err.Error())
	}

	err = db.PutWithExpiration([]byte("bigboss"), []byte("2063"), 10*time.Second)
	if err != nil {
		panic(err.Error())
	}

	os.RemoveAll(option.Path)
}
