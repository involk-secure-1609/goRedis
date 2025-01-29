package main

import "strconv"

var Handlers = map[string]func([]Value) Value{
	"PING":      ping,
	// string commads
	"SET":       set,
	"GET":       get,
	"MGET":      mget,
	"INCR":      incr,
	"INCRBY":    incrby,
	// hash commands
	"HSET":      hset,
	"HGET":      hget,
	"HGETALL":   hgetall,
	"LPUSH":     lpush,
	"LPOP":      lpop,
	"LLEN":      llen,
	"LINDEX":    lindex,
	"LRANGE":    lrange,
	"RPUSH":     rpush,
	"RPOP":      rpop,
	"SADD":      sadd,
	"SREM":      srem,
	"SCARD":     scard,
	"SISMEMBER": sismember,
}

// we have separate mutexes for the SET MAP AND THE HSET MAP
// MUTEXES WILL MAKE THE MAP SAFE
// HSET MAP IS A NESTED HASH MAP
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

func srem(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "string", str: "ERR wrong number of arguments for 'srem' command"}
	}
	key := args[0].bulk
	members := make([]string, 0)
	for i := 1; i < len(args); i++ {
		members = append(members, args[i].bulk)
	}
	elementsRemoved := ds_srem(key, members)
	return Value{typ: "string", str: elementsRemoved}
}
func sadd(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "string", str: "ERR wrong number of arguments for 'sadd' command"}
	}
	key := args[0].bulk
	members := make([]string, 0)
	for i := 1; i < len(args); i++ {
		members = append(members, args[i].bulk)
	}
	elementsAdded := ds_sadd(key, members)
	return Value{typ: "string", str: elementsAdded}
}
func scard(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "ERR wrong number of arguments for 'scard' command"}
	}
	key := args[0].bulk

	cardinality := ds_scard(key)
	return Value{typ: "string", str: cardinality}
}
func sismember(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "string", str: "ERR wrong number of arguments for 'sismember' command"}
	}
	key := args[0].bulk
	member := args[1].bulk
	isMember := ds_sismember(key, member)
	return Value{typ: "string", str: isMember}
}
func lrange(args []Value) Value {
	// if len(args) != 2 {
	// 	return Value{typ: "error", str: "ERR wrong number of arguments for 'lindex' command"}
	// }
	// key := args[0].bulk
	// index:=args[1].bulk
	// value, ok := ds_lindex(key,index)
	// if !ok {
	// 	return Value{typ: "null"}
	// }
	return Value{typ: "string", str: "value"}
}

func lindex(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lindex' command"}
	}
	key := args[0].bulk
	index := args[1].bulk
	value, ok := ds_lindex(key, index)
	if !ok {
		return Value{typ: "null"}
	}
	return Value{typ: "string", str: value}
}
func llen(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpop' command"}
	}
	key := args[0].bulk
	value := ds_llen(key)

	return Value{typ: "string", str: value}
}
func rpop(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpop' command"}
	}
	key := args[0].bulk
	value, ok := ds_rpop(key)
	if !ok {
		return Value{typ: "null"}
	}
	return Value{typ: "string", str: value}
}

func rpush(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpush' command"}
	}
	key := args[0].bulk
	values := make([]string, 0)
	for i := 1; i < len(args); i++ {
		values = append(values, args[i].bulk)
	}
	length := ds_rpush(key, values)
	return Value{typ: "string", str: length}
}
func lpop(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpop' command"}
	}
	key := args[0].bulk
	value, ok := ds_lpop(key)
	if !ok {
		return Value{typ: "null"}
	}
	return Value{typ: "string", str: value}
}
func lpush(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpush' command"}
	}
	key := args[0].bulk
	values := make([]string, 0)
	for i := 1; i < len(args); i++ {
		values = append(values, args[i].bulk)
	}
	length := ds_lpush(key, values)
	return Value{typ: "string", str: length}
}

func mget(args []Value) Value {
	if len(args)==0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'mget' command"}
	}

	keys := make([]string,0)
	for i:=0;i<len(args);i++{
		keys=append(keys, args[i].bulk)
	}
	value := ds_mget(keys)
	values := []Value{}
	for _, v := range value {
		values = append(values, Value{typ: "bulk", bulk: v})
	}
	return Value{typ: "array", array: values}
}
func incrby(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'incr' command"}
	}

	key := args[0].bulk
	increment:=args[1].bulk
	incr,err:=strconv.ParseInt(increment,10,64)
	if err!=nil{
		return Value{typ: "error", str: "ERR wrong value of increment provided"}
	}
	value := ds_incrby(key,incr)

	return Value{typ: "string", str: value}
}
func incr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'incr' command"}
	}

	key := args[0].bulk

	value := ds_incr(key)

	return Value{typ: "string", str: value}
}
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	ds_set(key, value)

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	value, ok := ds_get(key)

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	ds_hset(hash, key, value)
	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	value, ok := ds_hget(hash, key)

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, Value{typ: "bulk", bulk: k})
		values = append(values, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: values}
}
