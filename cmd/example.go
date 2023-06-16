package main

import (
	"bytes"
	"github.com/bigboss2063/ApexDB"
	"os"
)

func main() {
	option := ApexDB.DefaultOption()
	db, err := ApexDB.OpenDB(option)
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
	if err != ApexDB.ErrKeyNotExist {
		panic(err.Error())
	}

	os.RemoveAll(option.Path)
}
