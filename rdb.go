package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Rdb struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

const (
	MAGIC               = "REDIS"
	VERSION             = "0001"
	SELECT_DB            = 0xFE
	RDB_EOF              = 0xFF
	StringValueEncoding = 0
	ListValueEncoding   = 1
	SetValueEncoding    = 2
	HashValueEncoding   = 4
)

// var StringValueEncoding int=0
// var ListValueEncoding int =1
// var SetValueEncoding int =2
// var HashValueEncoding int=4

func NewRbd(path string) (*Rdb, error) {
	// 0666 pem permission gives every one read and write access to the file
	// os.O_CREATE|os._RDWR creates if it is not present
	// otherwise it opens the file with read and write permissions
	// this is there in the docs
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	rbd := &Rdb{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// start go routine to sync aof to disk every 1 second
	go func() {
		for {
			rbd.mu.Lock()
			err := rbd.write()
			// os.Rename()
			log.Fatalln("Writing to rbd failed", err)
			time.Sleep(time.Second)
		}
	}()

	return rbd, nil
}

// func (rbd *Rdb) deserializeLength(offset int) (int, error) {
// 	return 1, nil
// }

func writeConstants(temp *os.File, offset *int) {
	var n int
	n, _ = temp.WriteAt([]byte(MAGIC), int64(*offset))
	*offset += n
	n, _ = temp.WriteAt([]byte(VERSION), int64(*offset))
	*offset += n
	n, _ = temp.WriteAt([]byte{SELECT_DB}, int64(*offset))
	*offset += n

	databaseSelector := serializeLength(1)
	n, _ = temp.WriteAt(databaseSelector, int64(*offset))
	*offset += n

}

func writeStrings(temp *os.File, offset *int) {
	var n int
	StringSETSMu.Lock()
	n, _ = temp.WriteAt(serializeLength(StringValueEncoding), int64(*offset))
	*offset += n
	for key, value := range StringSETS {
		keyBytes := serializeString(key)
		valueBytes := serializeString(value)
		stringBytes := make([]byte, 0)
		// Preallocate memory for stringBytes
		stringBytes = append(stringBytes, keyBytes...)
		stringBytes = append(stringBytes, valueBytes...)
		n, err := temp.WriteAt(stringBytes, int64(*offset))
		if err != nil {
			break
		}
		*offset += n
	}
	StringSETSMu.Unlock()
}

func writeLists(temp *os.File, offset *int) {
	var n int
	LISTSMu.Lock()
	for key, list := range LISTS {
		values := ds_ltrav(list)
		n, _ = temp.WriteAt(serializeLength(ListValueEncoding), int64(*offset))
		*offset += n
		keyBytes := serializeString(key)
		n, _ = temp.WriteAt(keyBytes, int64(*offset))
		*offset += n
		length := len(values)
		lengthBytes := serializeLength(length)
		n, _ = temp.WriteAt(lengthBytes, int64(*offset))
		*offset += n
		for i := 0; i < length; i++ {
			valueBytes := serializeString(values[i])
			n, _ = temp.WriteAt(valueBytes, int64(*offset))
			*offset += n
		}
	}
	LISTSMu.Unlock()
}

func writeSets(temp *os.File, offset *int) {
	SETsMu.Lock()
	var n int
	for key, list := range SETs {
		members := ds_strav(list)
		n, _ = temp.WriteAt(serializeLength(SetValueEncoding), int64(*offset))
		*offset += n
		keyBytes := serializeString(key)
		n, _ = temp.WriteAt(keyBytes, int64(*offset))
		*offset += n
		length := len(members)
		lengthBytes := serializeLength(length)
		n, _ = temp.WriteAt(lengthBytes, int64(*offset))
		*offset += n
		for i := 0; i < length; i++ {
			valueBytes := serializeString(members[i])
			n, _ = temp.WriteAt(valueBytes, int64(*offset))
			*offset += n
		}
	}
	SETsMu.Unlock()
}
func (rbd *Rdb) write() error {
	temp, err := os.OpenFile("database_temp.rbd", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	temp.Seek(0, io.SeekStart)
	offset := 0
	writeConstants(temp, &offset)
	writeStrings(temp, &offset)
	writeLists(temp, &offset)
	writeSets(temp, &offset)
	// n, _ = temp.WriteAt([]byte(MAGIC), int64(offset))
	// offset += n
	// n, _ = temp.WriteAt([]byte(VERSION), int64(offset))
	// offset += n
	// n, _ = temp.WriteAt([]byte{SELECTDB}, int64(offset))
	// offset += n

	// databaseSelector := serializeLength(1)
	// n, _ = temp.WriteAt(databaseSelector, int64(offset))
	// offset += n

	// write all the keys and values in the StringSet
	// StringSETSMu.Lock()
	// n, _ = temp.WriteAt(serializeLength(StringValueEncoding), int64(offset))
	// offset += n
	// for key, value := range StringSETS {
	// 	keyBytes := serializeString(key)
	// 	valueBytes := serializeString(value)
	// 	stringBytes := make([]byte, 0)
	// 	// Preallocate memory for stringBytes
	// 	stringBytes = append(stringBytes, keyBytes...)
	// 	stringBytes = append(stringBytes, valueBytes...)
	// 	n, err = temp.WriteAt(stringBytes, int64(offset))
	// 	if err != nil {
	// 		break
	// 	}
	// 	offset += n
	// }
	// StringSETSMu.Unlock()

	// write the listMap
	// LISTSMu.Lock()
	// for key, list := range LISTS {
	// 	values := ds_ltrav(list)
	// 	n, _ = temp.WriteAt(serializeLength(ListValueEncoding), int64(offset))
	// 	offset += n
	// 	keyBytes := serializeString(key)
	// 	n, _ = temp.WriteAt(keyBytes, int64(offset))
	// 	offset += n
	// 	length := len(values)
	// 	lengthBytes := serializeLength(length)
	// 	n, _ = temp.WriteAt(lengthBytes, int64(offset))
	// 	offset += n
	// 	for i := 0; i < length; i++ {
	// 		valueBytes := serializeString(values[i])
	// 		n, _ = temp.WriteAt(valueBytes, int64(offset))
	// 		offset += n
	// 	}
	// }
	// LISTSMu.Unlock()

	// write the listMap
	// SETsMu.Lock()
	// for key, list := range SETs {
	// 	values := ds_ltrav(list)
	// 	n, _ = temp.WriteAt(serializeLength(ListValueEncoding), int64(offset))
	// 	offset += n
	// 	keyBytes := serializeString(key)
	// 	n, _ = temp.WriteAt(keyBytes, int64(offset))
	// 	offset += n
	// 	length := len(values)
	// 	lengthBytes := serializeLength(length)
	// 	n, _ = temp.WriteAt(lengthBytes, int64(offset))
	// 	offset += n
	// 	for i := 0; i < length; i++ {
	// 		valueBytes := serializeString(values[i])
	// 		n, _ = temp.WriteAt(valueBytes, int64(offset))
	// 		offset += n
	// 	}
	// }
	// SETsMu.Unlock()

	return nil
}
