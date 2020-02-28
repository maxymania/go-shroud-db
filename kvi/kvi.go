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


package kvi

import "errors"

var (
	ErrTooManyRequests  = errors.New("TooManyRequests")
	ErrKeyNotFound      = errors.New("KeyNotFound")
	ErrKeyAlreadyExists = errors.New("KeyAlreadyExists")
	ErrInvalidKey       = errors.New("InvalidKey")
	ErrInvalidData      = errors.New("InvalidData")
)

type Entry struct {
	Key,Value []byte
	ExpiresAt uint64 // time.Unix
}
/*
func (e Entry) String() string {
	return fmt.Sprintf("{%q:%q Expires At %d}",e.Key,e.Value,e.ExpiresAt)
}
*/


// Unstable: to be extended
type KeyValueStore interface {
	SetEntry(e *Entry) error
	RevertSet(key []byte) error
	HasValue(key []byte) error
	GetValue(key []byte,cb func(val []byte)error) error
}

// #
