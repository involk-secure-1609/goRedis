package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Rdb struct {
	file *os.File
	mu   sync.Mutex
}

const (
	MAGIC               = "REDIS" // The MAGIC FLAG has to be the first bytes written to the rdb
	VERSION             = "0001"  // The VERSION FLAG will then be written to the file
	SELECT_DB           = 0xFE    // The SELECT_DB FLAG is used to indicate that a database serialization follows
	DATABASE_NO         = 1       // The DatabaseNo will follow the SELECT_DB FLAG
	RDB_EOF             = 0xFF    // The RDB_EOF FLAG indicates the end of the rdb file
	StringValueEncoding = 0       // Indicates the following value encoding is of String type
	ListValueEncoding   = 1       // Indicates the following value encoding is of List type
	SetValueEncoding    = 2       // Indicates the following value encoding is of Set type
	HashValueEncoding   = 4       // Indicates the following value encoding is of Hash type
)

func NewRbd(path string) (*Rdb, error) {
	// 0666 pem permission gives every one read and write access to the file
	// os.O_CREATE|os._RDWR creates if it is not present
	// otherwise it opens the file with read and write permissions
	// this is there in the docs
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)

	// If we are unable to open the rdb file , then there is something from with our
	// database so we should not continue to serve requests
	if err != nil {
		panic(err)
	}

	rdb := &Rdb{
		file: f,
	}
	err = rdb.load()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	// start go routine to overwrite the rdb and save to disk every 60 seconds
	go func() {
		for {
			time.Sleep(10 * time.Second)
			log.Println("starting to write rdb")
			err := rdb.write()
			if err != nil {
				log.Println(err)
				panic(err)
			}
			log.Println("finished writing rdb")
		}
	}()

	return rdb, nil
}

func (rdb *Rdb) Close() error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	return rdb.file.Close()
}

func (rdb *Rdb) write() error {
	log.Println("entering into the RdbWrite function")
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	// we open the file
	log.Println("trying to open file")
	temp, err := os.OpenFile("database.rdb", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("opened file")
	// move to the beginning of the file
	temp.Seek(0, io.SeekStart)
	offset := 0
	// first we write the constants
	err = writeConstants(temp, &offset)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("wrote constants")

	// then we write the StringSETs datastructure
	err = writeStrings(temp, &offset)
	if err != nil {
		return err
	}
	log.Println("wrote strings successfully")

	// then we write the LISTS datastructure
	err = writeLists(temp, &offset)
	if err != nil {
		return err
	}
	log.Println("wrote lists successfully")

	// then we write the SETs datastucture
	err = writeSets(temp, &offset)
	if err != nil {
		return err
	}
	log.Println("wrote sets successfully")

	// then we write the HSETs datastructure
	err = writeHash(temp, &offset)
	if err != nil {
		return err
	}
	log.Println("wrote hash successfully")

	// then we write the RdbEOF flag
	err = writeRdb_Eof(temp, &offset)
	if err != nil {
		return err
	}
	log.Println("wrote rdb eof successfully")

	// we make sure we flush the file to disk
	err = temp.Sync()
	if err != nil {
		log.Println(err)
		return err
	}
	// // we atomically rename the database_temp.rdb to database.rbd
	// err = renameRbd()
	// if err != nil {
	// 	log.Println(err)
	// 	return err
	// }
	// log.Println("finished renaming")
	return nil
}

func (rdb *Rdb) load() error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	curr, err := os.OpenFile("database.rdb", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	fileInfo, err := curr.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	curr.Seek(0, io.SeekStart)
	offset := 0
	if fileInfo.Size() == 0 {
		log.Println("File is empty, initializing with default data...")
		writeConstants(curr, &offset)
		writeRdb_Eof(curr, &offset)
		return nil
		// Initialize the file with default data or perform other actions
	}
	curr.Seek(0, io.SeekStart)
	offset = 0
	err = readConstants(curr, &offset)
	log.Println("Offset: ", offset)
	if err != nil {
		log.Println("Failed to read constants lol")
		return err
	}
	log.Println("read all the constants...")
	// for {
	byteRead, err := readRdbByte(curr, &offset)
	if err != nil {
		return err
	}
	if byteRead == SELECT_DB {
		log.Println("read SELECT_DB FLAG")
	}
	// }
	log.Println("Offset: ", offset)
	databaseNumber, n, _, err := readRdbLength(curr, offset)
	log.Println(databaseNumber)
	if err != nil {
		return err
	}
	offset += n
	log.Println("Offset: ", offset)
	for {
		valueEncoding, n, end, err := readRdbLength(curr, offset)
		log.Println(valueEncoding, n, end, err)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if end {
			log.Println("read the RDB EOF")
			return nil
		}
		offset += n
		log.Println("Offset: ", offset)
		if valueEncoding == SetValueEncoding {
			log.Println("read value encoding of Set", valueEncoding)
			err = readRdbSet(curr, &offset)
			if err != nil {
				return err
			}
		} else if valueEncoding == HashValueEncoding {
			log.Println("read value encoding of Hash", valueEncoding)
			err = readRdbHash(curr, &offset)
			if err != nil {
				return err
			}
		} else if valueEncoding == ListValueEncoding {
			log.Println("read value encoding of List", valueEncoding)
			err = readRdbList(curr, &offset)
			if err != nil {
				return err
			}
		} else if valueEncoding == StringValueEncoding {
			log.Println("read value encoding of String", valueEncoding)
			err = readRdbStringSet(curr, &offset)
			if err != nil {
				log.Println("finished reading stringSet with", err)
				return err
			}
		}

	}
}

func writeConstants(temp *os.File, offset *int) error {
	// writes the MAGIC FLAG
	constantBytes := make([]byte, 0)
	n, err := temp.WriteAt([]byte(MAGIC), int64(*offset))
	if err != nil {
		return err
	}
	constantBytes = append(constantBytes, []byte(MAGIC)...)
	log.Println("Offset: ", *offset)
	*offset += n
	log.Println("Offset: ", *offset)

	// writes the VERSION FLAG
	n, err = temp.WriteAt([]byte(VERSION), int64(*offset))
	if err != nil {
		return err
	}
	*offset += n
	log.Println("Offset: ", *offset)
	constantBytes = append(constantBytes, []byte(VERSION)...)

	// writes the SELECT_DB FLAG
	n, err = temp.WriteAt([]byte{SELECT_DB}, int64(*offset))
	if err != nil {
		return err
	}
	*offset += n
	log.Println("Offset: ", *offset)
	constantBytes = append(constantBytes, []byte{SELECT_DB}...)

	// Then we write the DATABASE no
	databaseSelector := serializeLength(DATABASE_NO)
	n, err = temp.WriteAt(databaseSelector, int64(*offset))
	if err != nil {
		return err
	}
	constantBytes = append(constantBytes, databaseSelector...)
	*offset += n
	log.Println("Offset: ", *offset)
	log.Println(constantBytes)
	return nil
}

// Function which writes the StringSETS DataStructure to the Rdb file
func writeStrings(temp *os.File, offset *int) error {
	StringSETSMu.Lock()
	defer StringSETSMu.Unlock()
	// We write each key and value using String Encoding
	if len(StringSETS) > 0 {
		for key, value := range StringSETS {
			// We first write the StringValue
			valueEncoding := serializeLength(StringValueEncoding)
			log.Println(valueEncoding, len(valueEncoding))
			n, err := temp.WriteAt(valueEncoding, int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			keyBytes := serializeString(key)
			log.Println(keyBytes, len(keyBytes))
			valueBytes := serializeString(value)
			log.Println(valueBytes, len(valueBytes))
			stringBytes := make([]byte, 0)
			// Concatenate the key and value serialization
			// so in case we fail in the middle we wont have partially written
			// a key without writing a value
			stringBytes = append(stringBytes, keyBytes...)
			stringBytes = append(stringBytes, valueBytes...)
			log.Println(stringBytes, len(stringBytes))
			n, err = temp.WriteAt(stringBytes, int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
		}
	}
	return nil
}

// Function which writes the LISTS DataStructure to the Rdb file
func writeLists(temp *os.File, offset *int) error {
	LISTSMu.Lock()
	defer LISTSMu.Unlock()
	// We iterate through each key->List mapping in the LISTS Map
	for key, list := range LISTS {
		// Then we extract all the values in that particular list
		values := ds_ltrav(list)
		if len(values) > 0 {

			// We write the StringValueEncoding which idetifies
			// that the following key value is of String type
			n, err := temp.WriteAt(serializeLength(ListValueEncoding), int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			// Then we write the key which is basically the name of the List
			keyBytes := serializeString(key)
			n, err = temp.WriteAt(keyBytes, int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			length := len(values)
			// Then we write the length of the List
			lengthBytes := serializeLength(length)
			n, err = temp.WriteAt(lengthBytes, int64(*offset))
			if err != nil {
				return err
			}
			*offset += n

			// All the values in the List are being stored in the String encoding format
			for i := 0; i < length; i++ {
				valueBytes := serializeString(values[i])
				n, err = temp.WriteAt(valueBytes, int64(*offset))
				if err != nil {
					return err
				}
				*offset += n
			}
		}
	}
	return nil
}

// Function which writes the Sets DataStructure to the Rdb file
// Sets are written similarly as Lists
// We first write the Value Flag which identifies that the value is of SET Encoding
// Then we write the Set name as the key, the Size of the set in Length encoding and then
// all the members belonging to the Set as strings
func writeSets(temp *os.File, offset *int) error {
	SETsMu.Lock()
	defer SETsMu.Unlock()
	for key, list := range SETs {
		// extract all the members of the list
		members := ds_strav(list)
		if len(members) > 0 {
			// write the ValueType flag for a Set to the file
			n, err := temp.WriteAt(serializeLength(SetValueEncoding), int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			// write the key which is the set name to the file
			keyBytes := serializeString(key)
			n, err = temp.WriteAt(keyBytes, int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			length := len(members)
			// write the key which is the set name to the file
			lengthBytes := serializeLength(length)
			n, err = temp.WriteAt(lengthBytes, int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			for i := 0; i < length; i++ {
				valueBytes := serializeString(members[i])
				n, err = temp.WriteAt(valueBytes, int64(*offset))
				if err != nil {
					return err
				}
				*offset += n
			}
		}
	}
	return nil
}

// Function to write the Rdb EOF Flag at the end of the file
func writeRdb_Eof(temp *os.File, offset *int) error {
	n, err := temp.WriteAt([]byte{RDB_EOF}, int64(*offset))
	if err != nil {
		return err
	}
	*offset += n
	return nil
}

// Function for serializing Hash Value Encoding
func writeHash(temp *os.File, offset *int) error {
	HSETsMu.Lock()
	defer HSETsMu.Unlock()
	// traversing through the key(names of the hashes) and the hash
	for key, hash := range HSETs {
		// extracting all the members of the hash
		// members is an array of HashElement stuct which contains
		// both the key and value
		members := ds_htrav(hash)
		if len(members) > 0 {
			// write the ValueType flag for a Hash to the file
			n, err := temp.WriteAt(serializeLength(HashValueEncoding), int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			// write the key which is the hash name to the file
			keyBytes := serializeString(key)
			n, err = temp.WriteAt(keyBytes, int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			// write the length of the members of the file
			length := len(members)
			lengthBytes := serializeLength(length)
			n, err = temp.WriteAt(lengthBytes, int64(*offset))
			if err != nil {
				return err
			}
			*offset += n
			// then we traverse through all the elements of the members array
			// and write each key and value of the hash to the file as strings
			for i := 0; i < length; i++ {
				keyBytes := serializeString(members[i].key)
				valueBytes := serializeString(members[i].value)
				memberBytes := make([]byte, 0)
				// Preallocate memory for stringBytes
				memberBytes = append(memberBytes, keyBytes...)
				memberBytes = append(memberBytes, valueBytes...)
				n, err := temp.WriteAt(memberBytes, int64(*offset))
				if err != nil {
					return err
				}
				*offset += n
			}
		}
	}
	return nil
}

/*
Atomically renames the temporary rdb to the current rdb
os.Rename should be atomic hopefully
*/
func renameRbd() error {
	err := os.Rename("database_temp.rdb", "database.rdb")
	if err != nil {
		return err
	}
	return nil
}

func readRdbByte(file *os.File, offset *int) (byte, error) {
	byteRead := make([]byte, 1)
	n, err := file.ReadAt(byteRead, int64(*offset))
	log.Println(n, byteRead, byteRead[0])
	*offset += n
	if err != nil {
		return byteRead[0], err
	}
	return byteRead[0], nil
}

func readRdbLength(file *os.File, offset int) (result int, bytesRead int, eof bool, Err error) {
	// Read one byte from the stream
	byteRead := make([]byte, 1)
	_, err := file.ReadAt(byteRead, int64(offset))
	if err != nil {
		log.Println("Error reading file:", err)
		return -1, -1, false, err
	}
	if byteRead[0] == RDB_EOF {
		return -1, -1, true, nil
	}
	// Extract the two most significant bits
	msb := (byteRead[0] >> 6) & 0b11 // Shift right by 6 and mask to get the 2 MSBs

	// Determine how to parse the length based on the MSBs
	switch msb {
	case 0b00: // 00: The next 6 bits represent the length
		length := int(byteRead[0] & 0b00111111) // Mask to get the last 6 bits
		return length, 1, false, nil

	case 0b01: // 01: Read one additional byte. The combined 14 bits represent the length
		nextByte := make([]byte, 1)
		_, err := file.ReadAt(nextByte, int64(offset)+1)
		if err != nil {
			log.Println("Error reading additional byte:", err)
			return -1, -1, false, nil
		}
		// Combine the last 6 bits of the first byte and all 8 bits of the second byte
		length := int(byteRead[0]&0b00111111)<<8 | int(nextByte[0])
		return length, 2, false, nil

	case 0b10: // 10: Discard the remaining 6 bits. The next 4 bytes represent the length
		lengthBytes := make([]byte, 4)
		_, err := file.ReadAt(lengthBytes, int64(offset)+1)
		if err != nil {
			log.Println("Error reading 4 bytes for length:", err)
			return -1, -1, false, nil
		}
		// Convert the 4 bytes to a 32-bit integer (big-endian)
		length := int(binary.BigEndian.Uint32(lengthBytes))
		return length, 5, false, nil

	case 0b11: // 11: Reserved or special case (not specified in your description)
		log.Println("Special case (11) not implemented")
		return -1, -1, false, nil

	default:
		log.Println("Invalid MSB value")
		return -1, -1, false, nil
	}
}

func readRdbSet(file *os.File, offset *int) error {
	SETsMu.Lock()
	defer SETsMu.Unlock()

	// Read key size
	keySize, n, _, err := readRdbLength(file, *offset)
	log.Println(keySize, n, err)
	if err != nil {
		return err
	}
	*offset += n

	// Read key
	key := make([]byte, keySize)
	log.Println(string(key))
	n, err = file.ReadAt(key, int64(*offset))
	if err != nil {
		return err
	}
	*offset += n

	// Read set size
	setSize, n, _, err := readRdbLength(file, *offset)
	if err != nil {
		return err
	}
	*offset += n

	// Initialize the set if it doesn't exist
	if SETs[string(key)] == nil {
		SETs[string(key)] = make(map[string]bool)
	}

	// Read each value in the set
	for i := 0; i < setSize; i++ {
		valueSize, n, _, err := readRdbLength(file, *offset)
		if err != nil {
			return err
		}
		*offset += n

		value := make([]byte, valueSize)
		n, err = file.ReadAt(value, int64(*offset))
		if err != nil {
			return err
		}
		*offset += n

		// Add the value to the set
		SETs[string(key)][string(value)] = true
	}

	return nil
}
func readRdbHash(file *os.File, offset *int) error {
	HSETsMu.Lock()
	defer HSETsMu.Unlock()
	hashNameSize, n, _, err := readRdbLength(file, *offset)
	if err != nil {
		return err
	}
	*offset += n
	hashName := make([]byte, hashNameSize)
	n, err = file.ReadAt(hashName, int64(*offset))
	if err != nil {
		return err
	}
	// Initialize the set if it doesn't exist
	if HSETs[string(hashName)] == nil {
		HSETs[string(hashName)] = make(map[string]string)
	}

	*offset += n
	hashSize, n, _, err := readRdbLength(file, *offset)
	if err != nil {
		return err
	}
	*offset += n
	for i := 0; i < hashSize; i++ {
		keySize, n, _, err := readRdbLength(file, *offset)
		if err != nil {
			return err
		}
		*offset += n
		key := make([]byte, keySize)
		n, err = file.ReadAt(key, int64(*offset))
		if err != nil {
			return err
		}
		*offset += n
		valueSize, n, _, err := readRdbLength(file, *offset)
		if err != nil {
			return err
		}
		*offset += n
		value := make([]byte, valueSize)
		n, err = file.ReadAt(value, int64(*offset))
		if err != nil {
			return err
		}
		*offset += n
		HSETs[string(hashName)][string(key)] = string(value)
	}
	return nil
}

func readRdbList(file *os.File, offset *int) error {
	// LISTSMu.Lock()
	// defer LISTSMu.Unlock()
	keySize, n, _, err := readRdbLength(file, *offset)
	if err != nil {
		return err
	}
	*offset += n
	key := make([]byte, keySize)
	n, err = file.ReadAt(key, int64(*offset))
	if err != nil {
		return err
	}
	*offset += n
	setSize, n, _, err := readRdbLength(file, *offset)
	if err != nil {
		return err
	}
	*offset += n
	values := make([]string, 0)
	for i := 0; i < setSize; i++ {
		valueSize, n, _, err := readRdbLength(file, *offset)
		if err != nil {
			return err
		}
		*offset += n
		value := make([]byte, valueSize)
		n, err = file.ReadAt(value, int64(*offset))
		if err != nil {
			return err
		}
		*offset += n
		values = append(values, string(value))
	}
	ds_rpush(string(key), values)
	return nil
}
func readConstants(file *os.File, offset *int) error {
	magicBytes := make([]byte, len(MAGIC))
	n, err := file.ReadAt(magicBytes, int64(*offset))
	if err != nil {
		return err
	}
	log.Print(magicBytes)
	*offset += n
	versionBytes := make([]byte, len(VERSION))
	n, err = file.ReadAt(versionBytes, int64(*offset))
	if err != nil {
		return err
	}
	log.Print(versionBytes)
	*offset += n
	return nil
}
func readRdbStringSet(file *os.File, offset *int) error {
	log.Println("Offset: ", offset)
	keySize, n, _, err := readRdbLength(file, *offset)
	log.Println(keySize, n, err)
	if err != nil {
		return err
	}
	*offset += n
	log.Println("Offset: ", offset)
	key := make([]byte, keySize)
	n, err = file.ReadAt(key, int64(*offset))
	if err != nil {
		log.Println("error while reading key", err)
		// return err
	}
	log.Println(n, err, len(key), string(key))
	*offset += n
	valueSize, n, _, err := readRdbLength(file, *offset)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println(valueSize, n, err)
	log.Println("finished reading value length", valueSize)
	*offset += n
	value := make([]byte, valueSize)
	n, err = file.ReadAt(value, int64(*offset))
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("finished reading value", value, string(value))
	*offset += n
	ds_set(string(key), string(value))
	return nil
}
