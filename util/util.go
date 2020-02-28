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


package util

import (
	"container/ring"
	"sync"
	"sync/atomic"
	
	"github.com/maxymania/go-shroud-db/kvi"
	"github.com/byte-mug/golibs/bufferex"
	"github.com/byte-mug/golibs/buffer"
)

func docopy(key *[]byte,slot **[]byte) {
	if len(*key)==0 {
		*key = nil
		*slot = nil
		return
	}
	bin := bufferex.NewBinary(*key)
	*key = bin.Bytes()
	*slot = bin.BufferAllocUnit()
}

var entry2Pool sync.Pool

var ringPool = sync.Pool{New:func()interface{}{
	return ring.New(1)
}}

type Entry2Pool struct {
	inner sync.Pool
}
func (p *Entry2Pool) Alloc() (e *Entry2) {
	if p!=nil {
		e,_ = p.inner.Get().(*Entry2)
	}
	if e==nil { e = new(Entry2) }
	e.pool = p
	return
}

type Entry2 struct {
	rc int32
	free [2]*[]byte
	pool *Entry2Pool
	
	kvi.Entry
}
func (e *Entry2) Grab() *Entry2 {
	atomic.AddInt32(&e.rc,1)
	return e
}
func (e *Entry2) Free() {
	if atomic.AddInt32(&e.rc,-1) !=0 { return }
	buffer.Put(e.free[0])
	buffer.Put(e.free[1])
	pool := e.pool
	if pool==nil { return }
	*e = Entry2{}
	pool.inner.Put(e)
}

func (e *Entry2) Set(oe *kvi.Entry) *Entry2 {
	if atomic.LoadInt32(&e.rc)!=0 { panic("BUG: Entry still in use") }
	atomic.AddInt32(&e.rc,1)
	e.Entry = *oe
	docopy(&e.Key,&e.free[0])
	docopy(&e.Value,&e.free[1])
	return e
}
func (e *Entry2) SetKey(key []byte) *Entry2 {
	if atomic.LoadInt32(&e.rc)!=0 { panic("BUG: Entry still in use") }
	atomic.AddInt32(&e.rc,1)
	e.Entry = kvi.Entry{Key:key}
	docopy(&e.Key,&e.free[0])
	docopy(&e.Value,&e.free[1])
	return e
}
func (e *Entry2) ListElem() (r *ring.Ring) {
	if e==nil { return nil }
	r = ringPool.Get().(*ring.Ring)
	r.Value = e
	return r
}

func FreeRingWithEntries(r *ring.Ring) {
	for {
		or := r // OldRing := Ring
		r = r.Next()
		if or!=r {
			or.Link(or) // Unlink OldRing from the Ring.
		}
		e,_ := or.Value.(*Entry2)
		if e!=nil { e.Free() }
		or.Value = nil
		ringPool.Put(or)
		if or==r { break }
	}
}

func convertFunc(f func(e *Entry2)) func(interface{}) {
	return func(i interface{}) {
		e,_ := i.(*Entry2)
		if e==nil { return }
		f(e)
	}
}
func Foreach(r *ring.Ring,f func(e *Entry2)) {
	r.Do(convertFunc(f))
}
func ListHead() *ring.Ring {
	return ringPool.Get().(*ring.Ring)
}

