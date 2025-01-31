package main

import (
	"encoding/binary"
	"log"
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
func serializeLength(length int) []byte {
	bytes := make([]byte, 0)
	if length < 64 {
		bytes = append(bytes, byte(length))
		log.Println(bytes)
		return bytes
	} else if length < 16383 {
		bytes = append(bytes, byte(length>>8), byte(length))
		bytes[0] = bytes[0] | (1 << 7)
		bytes[0] &^= (1 << 6)
		log.Println(bytes)
		return bytes
	}
	binary.BigEndian.AppendUint32(bytes, uint32(length))
	log.Println(bytes)
	return bytes
}

func serializeString(str string) []byte {
	bytes := make([]byte, 0)
	lengthBytes := serializeLength(len(str))
	bytes = append(bytes, lengthBytes...)
	bytes = append(bytes, []byte(str)...)
	return bytes
}
