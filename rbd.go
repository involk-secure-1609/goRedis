package main

import (
	"bufio"
	"os"
	"sync"
	"time"
)

type Rbd struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewRbd(path string) (*Rbd, error) {
	// 0666 pem permission gives every one read and write access to the file
	// os.O_CREATE|os._RDWR creates if it is not present
	// otherwise it opens the file with read and write permissions
	// this is there in the docs
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	rbd := &Rbd{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// start go routine to sync aof to disk every 1 second
	go func() {
		for {
			ds_acquireAllLocks()
			rbd.mu.Lock()

			// whenever we write to aof it is writing to the in_memory version
			// .Sync()
			/*
				Sync commits the current contents of the file
				to stable storage. Typically, this means flushing the file system's
				in-memory copy of recently written data to disk.
			*/
			rbd.file.Sync()
			ds_releaseAllLocks()
			rbd.mu.Unlock()

			time.Sleep(time.Second)
		}
	}()

	return rbd, nil
}
