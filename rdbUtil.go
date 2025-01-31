package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
)

func byteToBits(b byte) string {
	bits := ""
	for i := 7; i >= 0; i-- {
		// Check if the i-th bit is set
		if b&(1<<i) != 0 {
			bits += "1"
		} else {
			bits += "0"
		}
	}
	return bits
}

func bitsToByte(bits string) byte {
	var b byte
	for i, bit := range bits {
		if bit == '1' {
			// Set the (7 - i)-th bit
			b |= 1 << (7 - i)
		}
	}
	return b
}

func serializeValue(value string) (valueBytes []byte) {
	valueBytes = make([]byte, 0)
	num, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Println("Error parsing string:", err)
		valueBytes = append(valueBytes, serializeString(value)...)
		return valueBytes
	}
	switch {
	case num >= -128 && num <= 127:
		int8Num := int8(num)
		initialByte := byte(0)
		initialByte |= 1 << 7
		initialByte |= 1 << 6
		valueBytes = append(valueBytes, initialByte)
		log.Printf("Stored as int8: %d\n", int8Num)
		valueBytes = append(valueBytes, byte(int8Num))
		return valueBytes
	case num >= -32768 && num <= 32767:
		log.Printf("The number %d can be represented as an int16\n", num)
		int16Num := int16(num)
		initialByte := byte(1)
		initialByte |= 1 << 7
		initialByte |= 1 << 6
		valueBytes = append(valueBytes, initialByte)
		valueBytes = binary.BigEndian.AppendUint16(valueBytes, uint16(int16Num))
		log.Printf("Stored as int16: %d\n", int16Num)
		return valueBytes
	case num >= -2147483648 && num <= 2147483647:
		log.Printf("The number %d can be represented as an int32\n", num)
		int32Num := int32(num)
		initialByte := byte(2)
		initialByte |= 1 << 7
		initialByte |= 1 << 6
		valueBytes = append(valueBytes, initialByte)
		valueBytes = binary.BigEndian.AppendUint32(valueBytes, uint32(int32Num))
		log.Printf("Stored as int32: %d\n", int32Num)
		log.Println(valueBytes, len(valueBytes))
		return valueBytes
	default:
		log.Printf("The number %d must be represented as an int64\n", num)
		int64Num := int64(num)
		log.Printf("Stored as int64: %d\n", int64Num)
	}
	return nil
}

func readRdbLength(file *os.File, offset int) (result int,
	bytesRead int,
	eof bool,
	Err error,
	newDatabase bool,
	isActualInt bool,
) {
	// Read one byte from the stream
	byteRead := make([]byte, 1)
	_, err := file.ReadAt(byteRead, int64(offset))
	if err != nil {
		log.Println("Error reading file:", err)
		return -1, -1, false, err, false, false
	}
	if byteRead[0] == RDB_EOF {
		return -1, -1, true, nil, false, false
	}
	if byteRead[0] == SELECT_DB {
		return -1, -1, false, nil, true, false
	}
	// Extract the two most significant bits
	msb := (byteRead[0] >> 6) & 0b11 // Shift right by 6 and mask to get the 2 MSBs

	// Determine how to parse the length based on the MSBs
	switch msb {
	case 0b00: // 00: The next 6 bits represent the length
		length := int(byteRead[0] & 0b00111111) // Mask to get the last 6 bits
		return length, 1, false, nil, false, false

	case 0b01: // 01: Read one additional byte. The combined 14 bits represent the length
		nextByte := make([]byte, 1)
		_, err := file.ReadAt(nextByte, int64(offset)+1)
		if err != nil {
			log.Println("Error reading additional byte:", err)
			return -1, -1, false, err, false, false
		}
		// Combine the last 6 bits of the first byte and all 8 bits of the second byte
		length := int(byteRead[0]&0b00111111)<<8 | int(nextByte[0])
		return length, 2, false, nil, false, false

	case 0b10: // 10: Discard the remaining 6 bits. The next 4 bytes represent the length
		lengthBytes := make([]byte, 4)
		_, err := file.ReadAt(lengthBytes, int64(offset)+1)
		if err != nil {
			log.Println("Error reading 4 bytes for length:", err)
			return -1, -1, false, err, false, false
		}
		// Convert the 4 bytes to a 32-bit integer (big-endian)
		length := int(binary.BigEndian.Uint32(lengthBytes))
		return length, 5, false, nil, false, false

	case 0b11: // 11: Reserved or special case
		log.Println("Reached the special case where we read an actual INTEGER")
		typeOfNumber := int(byteRead[0] & 0b00111111) // Mask to get the last 6 bits
		log.Println(typeOfNumber)
		if typeOfNumber == 0 {
			log.Println("reached the type 0 of number reading")
			nextBytes := make([]byte, 1)
			_, err := file.ReadAt(nextBytes, int64(offset)+1)
			if err != nil {
				log.Println("Error reading 1 bytes for length:", err)
				return -1, -1, false, err, false, false
			}
			length := int(nextBytes[0])
			return length, 2, false, nil, false, true
		} else if typeOfNumber == 1 {
			log.Println("reached the type 1 of number reading")
			nextBytes := make([]byte, 2)
			_, err := file.ReadAt(nextBytes, int64(offset)+1)
			if err != nil {
				log.Println("Error reading 2 bytes for length:", err)
				return -1, -1, false, err, false, false
			}
			// Convert the 4 bytes to a 32-bit integer (big-endian)
			length := int(binary.BigEndian.Uint16(nextBytes))
			return length, 3, false, nil, false, true
		} else if typeOfNumber == 2 {
			log.Println("reached the type 2 of number reading")
			nextBytes := make([]byte, 4)
			_, err := file.ReadAt(nextBytes, int64(offset)+1)
			if err != nil {
				log.Println("Error reading 4 bytes for length:", err)
				return -1, -1, false, err, false, false
			}
			// Convert the 4 bytes to a 32-bit integer (big-endian)
			length := int(binary.BigEndian.Uint32(nextBytes))
			return length, 5, false, nil, false, true
		}
		return -1, -1, false, nil, false, false

	default:
		log.Println("Invalid MSB value")
		return -1, -1, false, fmt.Errorf("invalid MSB value"), false, false
	}
}

func serializeLength(length int) []byte {
	bytes := make([]byte, 0)
	if length < 64 {
		bytes = append(bytes, byte(length))
		log.Println(bytes)
		return bytes
	} else if length < 16383 {
		bytes = append(bytes, byte(length>>8), byte(length))
		bytes[0] = bytes[0] | (1 << 6)
		bytes[0] &^= (1 << 7)
		log.Println(bytes)
		return bytes
	}
	bytes = append(bytes, byte(0))
	bytes[0] = bytes[0] | (1 << 7)
	bytes[0] &^= (1 << 6)
	bytes = binary.BigEndian.AppendUint32(bytes, uint32(length))
	log.Println(len(bytes))
	return bytes
}

func serializeString(str string) []byte {
	bytes := make([]byte, 0)
	lengthBytes := serializeLength(len(str))
	bytes = append(bytes, lengthBytes...)
	bytes = append(bytes, []byte(str)...)
	return bytes
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
