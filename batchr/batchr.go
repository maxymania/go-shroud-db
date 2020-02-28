/*
Copyright (c) 2020 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package batchr

import (
	"github.com/maxymania/go-shroud-db/kvi"
	"github.com/maxymania/go-shroud-db/util"
	"github.com/dgraph-io/badger"
	"github.com/maxymania/go-unstable/bbolt"
	"sync"
	
	"container/ring"
	"time"
	"os"
)

type DB struct{
	// Valid all the time.
	perst *badger.DB
	cache *bbolt.DB  // transaction bypass cache
	
	queue  chan *util.Entry2
	txnsem chan int // A semaphore, limiting the number of maximum concurrent transactions.
	
	pool util.Entry2Pool
	commitErr error
}

var defbucket = []byte("default")

func swapError(err error) error {
	if err==badger.ErrKeyNotFound { err = kvi.ErrKeyNotFound }
	if err==badger.ErrInvalidKey { err = kvi.ErrInvalidKey }
	return err
}


type removeFrom struct{
	bbolt.VisitorDefault
}
func (removeFrom) VisitFull(key, value []byte) bbolt.VisitOp { return bbolt.VisitOpDELETE() }
var removeFromRef bbolt.Visitor = removeFrom{}

func (db *DB) cacheUpdate(fu func(tx *bbolt.Tx)error) error{
	return db.cache.Batch(fu)
}
func (db *DB) commitDone(reslist *ring.Ring) func(error) {
	return func(e error) {
		<- db.txnsem
		if e!=nil { db.commitErr = e }
		db.cacheUpdate(func(tx *bbolt.Tx)error {
			bkt := tx.Bucket(defbucket)
			util.Foreach(reslist,func(e *util.Entry2) { bkt.Accept(e.Key,removeFromRef,true) })
			return nil
		})
		util.FreeRingWithEntries(reslist)
	}
}

func (db *DB) worker() {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	ticks := 0
	
	var ent *util.Entry2
	var txn *badger.Txn
	var res *ring.Ring
	
	for {
		// Clear the pointer on every iteration.
		ent = nil
		
		if txn!=nil {
			/*
			If there is an active transaction, receive the next Entry with a timeout.
			*/
			select {
			case ent = <- db.queue:
			case <- ticker.C:
				/*
				Receive 3 ticker events, before timing out.
				- As the Channel usually is filled with an old event, the first event will be immediate.
				- The second event will propably come from a half-consumed timeslice (less than 1ms).
				- The third event will be the consumption of a full Timeslice (1ms).
				Putting all together, this results in a timeout of 1-3ms.
				*/
				ticks++
				if ticks<3 { continue }
			}
		} else {
			/*
			If no transaction is active, just wait for the next Entry.
			XXX: We assume, that the Entry is not <nil>
			*/
			ent = <- db.queue
		}
		ticks = 0
		
		if ent==nil {
			/* If we got here, we encountered a timeout. */
			txn.CommitWith(db.commitDone(res))
			txn = nil
			res = nil
			continue
		}
		if txn==nil {
			/* If there is no running transaction, start one. */
			db.txnsem <- 0
			txn = db.perst.NewTransaction(true)
			res = util.ListHead()
		}
		if len(ent.Value)==0 {
			/*
			If there is no value, this Entry is meant as a Delete-Request, to revert an earlier insert.
			*/
			err := txn.Delete(ent.Key)
			
			/* If the Transaction is full, commit and restart. */
			if err==badger.ErrTxnTooBig {
				txn.CommitWith(db.commitDone(res))
				db.txnsem <- 0
				txn = db.perst.NewTransaction(true)
				res = util.ListHead()
				txn.Delete(ent.Key)
			}
			// Let the GC deal with the Entry object!
			continue
		}
		
		bent := &badger.Entry{Key:ent.Key,Value:ent.Value,ExpiresAt:ent.ExpiresAt}
		err := txn.SetEntry(bent)
		
		/* If the Transaction is full, commit and restart. */
		if err==badger.ErrTxnTooBig {
			txn.CommitWith(db.commitDone(res))
			db.txnsem <- 0
			txn = db.perst.NewTransaction(true)
			res = util.ListHead()
			err = txn.SetEntry(bent)
		}
		// We are ignoring the error. We can't do anything here.
		res.Link(ent.ListElem())
	}
}

const invalidPrefix = "!badger!"
func isValidKey(b []byte) bool {
	if len(invalidPrefix)>len(b) { return false }
	return string(b[:len(invalidPrefix)])==invalidPrefix
}

type valueInstaller struct{
	bbolt.VisitorDefault
	e2 *util.Entry2
	success bool
}
func (v *valueInstaller) VisitBefore() { v.success = false }
func (v *valueInstaller) VisitEmpty(key []byte) bbolt.VisitOp {
	v.success = true
	return bbolt.VisitOpSET(v.e2.Value)
}

var valueInstallerPool = sync.Pool{New:func()interface{}{
	return new(valueInstaller)
}}
func (g *valueInstaller) free() {
	*g = valueInstaller{}
	valueInstallerPool.Put(g)
}

func (db *DB) SetEntry(e *kvi.Entry) error {
	if err := db.commitErr; err!=nil { return err }
	if isValidKey(e.Key) || len(e.Key)==0 { return kvi.ErrInvalidKey }
	if len(e.Value)==0 { return kvi.ErrInvalidData }
	e2 := db.pool.Alloc().Set(e)
	defer e2.Free()
	inst := valueInstallerPool.Get().(*valueInstaller)
	defer inst.free()
	inst.e2 = e2;
	
	/*
	Step 1: Install the Entry in the transaction bypass, to make it observable.
	*/
	db.cacheUpdate(func(tx *bbolt.Tx)error{
		tx.Bucket(defbucket).Accept(e2.Key,inst,true)
		return nil
	})
	
	// If an Entry with that key already existed in the transaction bypass, fail.
	if !inst.success { return kvi.ErrKeyAlreadyExists }
	
	/*
	Step 2: Submit the insertion to the Datastore.
	*/
	db.queue <- e2.Grab()
	return nil
}
func (db *DB) RevertSet(key []byte) error {
	if isValidKey(key) || len(key)==0 { return kvi.ErrInvalidKey }
	e2 := db.pool.Alloc().SetKey(key)
	
	/*
	Clear the Entry from the transaction bypass.
	*/
	db.cacheUpdate(func(tx *bbolt.Tx) error {
		tx.Bucket(defbucket).Accept(e2.Key,removeFromRef,true)
		return nil
	})
	
	/*
	Also clear the Entry from the Datastore.
	*/
	db.queue <- e2
	return nil
}


func (db *DB) HasValue(key []byte) error {
	ok := false
	// Check the transaction bypass for the entry.
	db.cache.View(func(tx *bbolt.Tx) error{
		ok = len(tx.Bucket(defbucket).Get(key))>0
		return nil
	})
	if ok { return nil }
	
	// Check the datastore
	return db.perst.View(func(txn *badger.Txn) error{
		_,err := txn.Get(key)
		return swapError(err)
	})
}

type getter struct {
	bbolt.VisitorDefault
	cb   func(val []byte)error
	err  error
	done bool
}
func (g *getter) VisitFull(key, value []byte) (vop bbolt.VisitOp) {
	if g.done { return }
	g.err = g.cb(value)
	g.done = true
	return
}
var getterPool = sync.Pool{New:func()interface{}{
	return new(getter)
}}
func (g *getter) free() {
	*g = getter{}
	getterPool.Put(g)
}

func (db *DB) GetValue(key []byte,cb func(val []byte)error) error {
	if cb==nil { panic("cb is nil") }
	g := getterPool.Get().(*getter)
	defer g.free()
	g.cb = cb
	
	// TODO: Can we perform a lookup on the datastore and the cache concurrently?
	
	/*
	Try to fetch the Entry from the transaction bypass.
	*/
	db.cache.View(func(tx *bbolt.Tx) error{
		return tx.Bucket(defbucket).Accept(key,g,false)
	})
	if g.done { return g.err }
	
	/*
	Try to fetch the Entry from the Datastore.
	*/
	return db.perst.View(func(txn *badger.Txn) error{
		item,err := txn.Get(key)
		if err!=nil { return swapError(err) }
		return item.Value(cb)
	})
}


func bboltOpts() *bbolt.Options {
	return &bbolt.Options{
		NoFreelistSync: true,
		InitialMmapSize: 1<<28, // 256 MB
		NoSync: true,
		DB_Flags: bbolt.DB_WriteSharedMmap,
	}
}

func badgerOpts(dir,valDir string) badger.Options {
	opts := badger.DefaultOptions(dir)
	if valDir!="" { opts.ValueDir = valDir }
	opts.ValueLogLoadingMode = 0 // FileIO
	return opts
}
func initCache(tx *bbolt.Tx) error {
	_,err := tx.CreateBucket(defbucket)
	return err
}

func Open(cache,dir,valDir string) (*DB,error) {
	os.Remove(cache)
	cdb,err := bbolt.Open(cache,0700,bboltOpts())
	if err!=nil { return nil,err }
	err = cdb.Update(initCache)
	if err!=nil { cdb.Close(); return nil,err }
	pdb,err := badger.Open(badgerOpts(dir,valDir))
	if err!=nil { cdb.Close(); return nil,err }
	
	db := &DB{
		perst:pdb,
		cache:cdb,
		queue: make(chan *util.Entry2,1<<10), // Max. 1024 pending Entries.
		txnsem: make(chan int,1<<7), // Max. 128 concurrent Txn-Commits.
	}
	go db.worker()
	
	return db,nil
}

var _ kvi.KeyValueStore = (*DB)(nil)
//var _ kvi.KeyValueStore = nil

// #
