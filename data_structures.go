package main

import (
	"strconv"
	"sync"
)

// Data structure representing a list
// Each list keeps track of its head and tail Node
// and also the total length of the list
// This allows O(1) lpop,lpush,rpop,rpush,llen time complexity
type List struct {
	head   *Node
	tail   *Node
	length int
}

// Data structure representing a node in the list
// Each node keeps track of its predecessor and successor Node
type Node struct {
	value string
	next  *Node
	prev  *Node
}

// Map for storing hashes
// In redis HashMaps are used for storing Hashes
/*
Commands:
HSET: sets the value of one or more fields on a hash.
HGET: returns the value at a given field.
HMGET: returns the values at one or more given fields.
HINCRBY: increments the value at a given field by the integer provided.
*/
var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

// Map for storing sets
// In redis HashMaps are used for storing Sets
/*
Commands:
SADD adds a new member to a set.
SREM removes the specified member from the set.
SISMEMBER tests a string for set membership.
SINTER returns the set of members that two or more sets have in common (i.e., the intersection).
SCARD returns the cardinality of the set
*/
var SETs = map[string]map[string]bool{}
var SETsMu = sync.RWMutex{}

// Map for storing strings
// In redis HashMaps are used for storing Sets
/*
Commands:
SET
GET
*/
var StringSETS = map[string]string{}
var StringSETSMu = sync.RWMutex{}

// Map for storing lists
// In redis LinkedLists are used for storing lists
/*
Commands:
LPUSH adds a new element to the head of a list; RPUSH adds to the tail.
LPOP removes and returns an element from the head of a list; RPOP does the same but from the tail of a list.
LLEN returns the length of a list.
LRANGE extracts a range of elements from a list.
*/
var LISTS = map[string]*List{}
var LISTSMu = sync.RWMutex{}

/*
General command to acquire and release locks over all the datastructures used
ds_acquireAllLocks is to be called before we write to the rdb
and ds_releaseAllLocks is to be called after we write to the rdb
*/
func ds_releaseAllLocks() {
	StringSETSMu.Unlock()
	HSETsMu.Unlock()
	SETsMu.Unlock()
	LISTSMu.Unlock()
}
func ds_acquireAllLocks() {
	StringSETSMu.Lock()
	HSETsMu.Lock()
	SETsMu.Lock()
	LISTSMu.Lock()
}

// Set Commands

func ds_strav(list map[string]bool) ([]string){
	members:=make([]string,0)
	for member,isFound:=range(list){
		if isFound{
			members = append(members, member)
		}
	}
	return members
}
func ds_scard(key string) string {
	SETsMu.Lock()
	defer SETsMu.Unlock()
	if _, ok := SETs[key]; !ok {
		return "0"
	}
	len := len(SETs[key])
	length := strconv.FormatInt(int64(len), 10)
	return length
}

func ds_srem(key string, members []string) string {
	SETsMu.Lock()
	defer SETsMu.Unlock()
	if _, ok := SETs[key]; !ok {
		return "0"
	}
	numberOfExistingElementsRemoved := 0
	for _, member := range members {
		_, ok := SETs[key][member]
		if ok {
			numberOfExistingElementsRemoved++
			delete(SETs[key], member)
		}
	}
	noOfElementsRemoved := strconv.FormatInt(int64(numberOfExistingElementsRemoved), 10)
	return noOfElementsRemoved

}

func ds_sismember(key string, member string) string {
	SETsMu.Lock()
	defer SETsMu.Unlock()
	if _, ok := SETs[key]; !ok {
		return "0"
	}
	_, ok := SETs[key][member]
	if !ok {
		return "0"
	}
	return "1"
}

func ds_sadd(key string, members []string) string {
	SETsMu.Lock()
	defer SETsMu.Unlock()
	if _, ok := SETs[key]; !ok {
		SETs[key] = map[string]bool{}
	}
	numberOfNewElementsAdded := 0
	for _, member := range members {
		_, ok := SETs[key][member]
		if !ok {
			SETs[key][member] = true
			numberOfNewElementsAdded++
		} else {
			SETs[key][member] = true
		}
	}
	noOfElementsAdded := strconv.FormatInt(int64(numberOfNewElementsAdded), 10)
	return noOfElementsAdded

}

// List Commands
func ds_rpush(key string, values []string) string {
	LISTSMu.Lock()
	defer LISTSMu.Unlock()
	list, ok := LISTS[key]
	if !ok {
		list = &List{head: nil, tail: nil, length: 0}
	}
	for _, value := range values {
		node := &Node{value: value, next: nil}
		if list.length == 0 {
			node.next = nil
			list.head = node
			list.tail = node
		} else {
			list.tail.next = node
			node.prev = list.tail
			list.tail = node
		}
		list.length++
	}
	length := strconv.FormatInt(int64(list.length), 10)
	return length
}
func ds_rpop(key string) (string, bool) {
	LISTSMu.Lock()
	defer LISTSMu.Unlock()
	list, ok := LISTS[key]
	if !ok {
		return "", false
	}
	tail := list.tail
	value := tail.value
	prev := tail.prev
	prev.next = nil
	list.tail = prev
	list.length--
	if list.length == 0 {
		delete(LISTS, key)
	}
	return value, true
}

func ds_lrange(key string, index string) (string, bool) {
	idx, _ := strconv.ParseInt(index, 10, 64)
	list, ok := LISTS[key]
	if !ok {
		return "", false
	}
	node := *list.head
	for i := 0; i < int(idx); i++ {
		node = *node.next
	}
	return node.value, true
}

func ds_ltrav(list *List) []string {
	values := make([]string, 0)
	node := *list.head
	size := list.length
	for i := 0; i < size; i++ {
		values = append(values, node.value)
		node = *node.next
	}
	return values

}
func ds_lindex(key string, index string) (string, bool) {
	idx, _ := strconv.ParseInt(index, 10, 64)
	list, ok := LISTS[key]
	if !ok {
		return "", false
	}
	node := *list.head
	for i := 0; i < int(idx); i++ {
		node = *node.next
	}
	return node.value, true
}
func ds_llen(key string) string {
	LISTSMu.Lock()
	defer LISTSMu.Unlock()
	list, ok := LISTS[key]
	if !ok {
		return "0"
	}
	length := strconv.FormatInt(int64(list.length), 10)
	return length
}
func ds_lpop(key string) (string, bool) {
	LISTSMu.Lock()
	defer LISTSMu.Unlock()
	list, ok := LISTS[key]
	if !ok {
		return "", false
	}
	head := list.head
	value := head.value
	next := head.next
	list.head = next
	list.length--
	if list.length == 0 {
		delete(LISTS, key)
	}
	return value, true
}
func ds_lpush(key string, values []string) string {
	LISTSMu.Lock()
	defer LISTSMu.Unlock()
	list, ok := LISTS[key]
	if !ok {
		list = &List{head: nil, tail: nil, length: 0}
	}
	for _, value := range values {
		node := &Node{value: value, prev: nil}
		if list.length == 0 {
			node.next = nil
			list.head = node
			list.tail = node
		} else {
			node.next = list.head
			list.head.prev = node
			list.head = node
		}
		list.length++
	}
	length := strconv.FormatInt(int64(list.length), 10)
	return length
}

// Hash commands
func ds_hget(hash string, key string) (string, bool) {
	HSETsMu.Lock()
	defer HSETsMu.Unlock()
	hashMap, ok := HSETs[hash]
	if !ok {
		return "", false
	}
	value, ok := hashMap[key]
	if !ok {
		return "", false
	}

	return value, true
}

func ds_hset(hash string, key string, value string) {
	HSETsMu.Lock()
	defer HSETsMu.Unlock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
}

// String Commands
func ds_set(key string, value string) {
	StringSETSMu.Lock()
	defer StringSETSMu.Unlock()
	StringSETS[key] = value
}

func ds_get(key string) (string, bool) {
	StringSETSMu.Lock()
	defer StringSETSMu.Unlock()
	v, ok := StringSETS[key]
	if !ok {
		return "", false
	}
	return v, true
}
func ds_mget(keys []string) []string {
	StringSETSMu.Lock()
	defer StringSETSMu.Unlock()
	values := make([]string, 0)
	for _, key := range keys {
		v, ok := StringSETS[key]
		if !ok {
			values = append(values, "nil")
		} else {
			values = append(values, v)
		}
	}
	return values
}

func ds_incr(key string) string {
	StringSETSMu.Lock()
	defer StringSETSMu.Unlock()

	val, ok := StringSETS[key]
	if !ok {
		return ""
	}

	// Parse existing value
	value, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return ""
	}

	value++
	StringSETS[key] = strconv.FormatInt(value, 10)
	return StringSETS[key]
}

func ds_incrby(key string, increment int64) string {
	StringSETSMu.Lock()
	defer StringSETSMu.Unlock()

	val, ok := StringSETS[key]
	if !ok {
		return ""
	}

	// Parse existing value
	value, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return ""
	}
	value += int64(increment)
	StringSETS[key] = strconv.FormatInt(value, 10)
	return StringSETS[key]
}
