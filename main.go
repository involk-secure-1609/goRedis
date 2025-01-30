package main

import (
	"fmt"
	"log"

	// "log"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	// Create a new server
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	// rdb, err := NewRbd("database.rdb")
	// if err != nil {
	// 	log.Println(err)
	// 	return
	// }
	// err = rdb.load()
	// if err != nil {
	// 	log.Println(err)
	// 	return
	// }
	// defer rdb.Close()
	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	// Listen for connections
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		log.Println(command)
		log.Println(args)
		writer := NewWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "error", str: fmt.Sprint("Invalid command: ", command)})
			continue
		}

		// if the command belongs to the aofSet which contains the set of commands to be written to
		// the aof then log it
		aofCommand, ok := aofSet[command]
		if ok {
			if aofCommand {
				aof.Write(value)
			}
		}
		result := handler(args)
		writer.Write(result)
	}
}
