package main

import (
	"bufio"
	"encoding/binary"
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
	SELECT_DB           = 0xFE
	RDB_EOF             = 0xFF
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

func readConstants(file *os.File, offset *int) {
	magicBytes := make([]byte, len(MAGIC))
	n, err := file.ReadAt(magicBytes, int64(*offset))
	if err != nil {
		return
	}
	*offset += n
	versionBytes := make([]byte, len(VERSION))
	n, err = file.ReadAt(versionBytes, int64(*offset))
	if err != nil {
		return
	}
	*offset += n
}

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

func writeRdb_Eof(temp *os.File, offset *int) {
	var n int
	n, _ = temp.WriteAt([]byte{RDB_EOF}, int64(*offset))
	*offset += n
}
func writeHash(temp *os.File, offset *int) {
	HSETsMu.Lock()
	// var n int
	for key, list := range HSETs {
		members := ds_htrav(list)
		if len(members) > 0 {
			n, err := temp.WriteAt(serializeLength(HashValueEncoding), int64(*offset))
			if err != nil {
				break
			}
			*offset += n
			keyBytes := serializeString(key)
			n, err = temp.WriteAt(keyBytes, int64(*offset))
			if err != nil {
				break
			}
			*offset += n
			length := len(members)
			lengthBytes := serializeLength(length)
			n, err = temp.WriteAt(lengthBytes, int64(*offset))
			if err != nil {
				break
			}
			*offset += n
			for i := 0; i < length; i++ {
				keyBytes := serializeString(members[i].key)
				valueBytes := serializeString(members[i].value)
				memberBytes := make([]byte, 0)
				// Preallocate memory for stringBytes
				memberBytes = append(memberBytes, keyBytes...)
				memberBytes = append(memberBytes, valueBytes...)
				n, err := temp.WriteAt(memberBytes, int64(*offset))
				if err != nil {
					break
				}
				*offset += n
			}
		}
	}
	HSETsMu.Unlock()
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
	writeHash(temp, &offset)
	writeRdb_Eof(temp, &offset)
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

func readRdbByte(file *os.File) byte {
	byteRead := make([]byte, 1)
	file.Read(byteRead)
	return byteRead[0]
}

func readRdbInt(file *os.File) (result int, bytesRead int, eof bool) {
	// Read one byte from the stream
	byteRead := make([]byte, 1)
	_, err := file.Read(byteRead)
	if err != nil {
		log.Println("Error reading file:", err)
		return -1, -1, false
	}
	if byteRead[0] == RDB_EOF {
		return -1, -1, true
	}
	// Extract the two most significant bits
	msb := (byteRead[0] >> 6) & 0b11 // Shift right by 6 and mask to get the 2 MSBs

	// Determine how to parse the length based on the MSBs
	switch msb {
	case 0b00: // 00: The next 6 bits represent the length
		length := int(byteRead[0] & 0b00111111) // Mask to get the last 6 bits
		return length, 1, false

	case 0b01: // 01: Read one additional byte. The combined 14 bits represent the length
		nextByte := make([]byte, 1)
		_, err := file.Read(nextByte)
		if err != nil {
			log.Println("Error reading additional byte:", err)
			return -1, -1, false
		}
		// Combine the last 6 bits of the first byte and all 8 bits of the second byte
		length := int(byteRead[0]&0b00111111)<<8 | int(nextByte[0])
		return length, 2, false

	case 0b10: // 10: Discard the remaining 6 bits. The next 4 bytes represent the length
		lengthBytes := make([]byte, 4)
		_, err := file.Read(lengthBytes)
		if err != nil {
			log.Println("Error reading 4 bytes for length:", err)
			return -1, -1, false
		}
		// Convert the 4 bytes to a 32-bit integer (big-endian)
		length := int(binary.BigEndian.Uint32(lengthBytes))
		return length, 5, false

	case 0b11: // 11: Reserved or special case (not specified in your description)
		log.Println("Special case (11) not implemented")
		return -1, -1, false

	default:
		log.Println("Invalid MSB value")
		return -1, -1, false
	}
}

func readRdbSet(file *os.File, offset *int) {
	SETsMu.Lock()
	defer SETsMu.Unlock()
	keySize, n, _ := readRdbInt(file)
	*offset += n
	key := make([]byte, keySize)
	n, err := file.ReadAt(key, int64(*offset))
	if err != nil {

	}
	*offset += n
	setSize, n, _ := readRdbInt(file)
	*offset += n
	for i := 0; i < setSize; i++ {
		valueSize, n, _ := readRdbInt(file)
		*offset += n
		value := make([]byte, valueSize)
		n, _ = file.ReadAt(value, int64(*offset))
		*offset += n
		SETs[string(key)][string(value)] = true
	}

}

func readRdbHash(file *os.File, offset *int) {
	HSETsMu.Lock()
	defer HSETsMu.Unlock()
	hashNameSize, n, _ := readRdbInt(file)
	*offset += n
	hashName := make([]byte, hashNameSize)
	n, err := file.ReadAt(hashName, int64(*offset))
	if err != nil {

	}
	*offset += n
	hashSize, n, _ := readRdbInt(file)
	*offset += n
	for i := 0; i < hashSize; i++ {
		keySize, n, _ := readRdbInt(file)
		*offset += n
		key := make([]byte, keySize)
		n, _ = file.ReadAt(key, int64(*offset))
		*offset += n
		valueSize, n, _ := readRdbInt(file)
		*offset += n
		value := make([]byte, valueSize)
		n, _ = file.ReadAt(value, int64(*offset))
		*offset += n
		HSETs[string(hashName)][string(key)] = string(value)
	}
}

func readRdbList(file *os.File, offset *int) {
	LISTSMu.Lock()
	defer LISTSMu.Unlock()
	keySize, n, _ := readRdbInt(file)
	*offset += n
	key := make([]byte, keySize)
	n, err := file.ReadAt(key, int64(*offset))
	if err != nil {

	}
	*offset += n
	setSize, n, _ := readRdbInt(file)
	*offset += n
	values := make([]string, 0)
	for i := 0; i < setSize; i++ {
		valueSize, n, _ := readRdbInt(file)
		*offset += n
		value := make([]byte, valueSize)
		n, _ = file.ReadAt(value, int64(*offset))
		*offset += n
		values = append(values, string(value))
	}
	ds_rpush(string(key), values)
}

func readRdbStringSet(file *os.File, offset *int) {
	StringSETSMu.Lock()
	defer StringSETSMu.Unlock()
	values := make([]string, 0)
	keySize, n, _ := readRdbInt(file)
	*offset += n
	key := make([]byte, keySize)
	n, err := file.ReadAt(key, int64(*offset))
	if err != nil {

	}
	*offset += n
	valueSize, n, _ := readRdbInt(file)
	*offset += n
	value := make([]byte, valueSize)
	n, _ = file.ReadAt(value, int64(*offset))
	*offset += n
	values = append(values, string(value))
	ds_sadd(string(key), values)
}
func (rbd *Rdb) load() error {
	curr, err := os.OpenFile("database.rbd", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	curr.Seek(0, io.SeekStart)
	offset := 0
	readConstants(curr, &offset)
	for {
		byteRead := readRdbByte(curr)
		offset++
		if byteRead == SELECT_DB {
			break
		}
	}
	for {
		valueEncoding, n, end := readRdbInt(curr)
		if end {
			return nil
		}
		offset += n
		if valueEncoding == SetValueEncoding {
			readRdbSet(curr, &offset)
		} else if valueEncoding == HashValueEncoding {
			readRdbHash(curr, &offset)
		} else if valueEncoding == ListValueEncoding {
			readRdbList(curr, &offset)
		} else if valueEncoding == StringValueEncoding {
			readRdbStringSet(curr, &offset)
		}

	}
}
