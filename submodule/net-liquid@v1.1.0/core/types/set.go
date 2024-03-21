/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	"sync"
	"sync/atomic"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
)

// Set .
type Set struct {
	size int32
	m    sync.Map
}

// Put .
func (s *Set) Put(v interface{}) bool {
	_, ok := s.m.LoadOrStore(v, struct{}{})
	if !ok {
		atomic.AddInt32(&s.size, 1)
	}
	return !ok
}

// Remove .
func (s *Set) Remove(v interface{}) bool {
	_, ok := s.m.LoadAndDelete(v)
	if ok {
		atomic.AddInt32(&s.size, -1)
	}
	return ok
}

// Exist .
func (s *Set) Exist(v interface{}) bool {
	if s == nil {
		return false
	}
	_, ok := s.m.Load(v)
	return ok
}

// Range .
func (s *Set) Range(f func(v interface{}) bool) {
	s.m.Range(func(key, _ interface{}) bool {
		return f(key)
	})
}

// Size .
func (s *Set) Size() int {
	return int(atomic.LoadInt32(&s.size))
}

// Uint64Set .
type Uint64Set struct {
	s Set
}

// Put .
func (us *Uint64Set) Put(v uint64) bool {
	return us.s.Put(v)
}

// Remove .
func (us *Uint64Set) Remove(v uint64) bool {
	return us.s.Remove(v)
}

// Exist .
func (us *Uint64Set) Exist(v uint64) bool {
	return us.s.Exist(v)
}

// Size .
func (us *Uint64Set) Size() int {
	return us.s.Size()
}

// Range .
func (us *Uint64Set) Range(f func(v uint64) bool) {
	us.s.Range(func(v interface{}) bool {
		uv, _ := v.(uint64)
		return f(uv)
	})
}

// StringSet .
type StringSet struct {
	s Set
}

// Put .
func (ss *StringSet) Put(str string) bool {
	return ss.s.Put(str)
}

// Remove .
func (ss *StringSet) Remove(str string) bool {
	return ss.s.Remove(str)
}

// Exist .
func (ss *StringSet) Exist(str string) bool {
	return ss.s.Exist(str)
}

// Size .
func (ss *StringSet) Size() int {
	return ss.s.Size()
}

// Range .
func (ss *StringSet) Range(f func(str string) bool) {
	ss.s.Range(func(v interface{}) bool {
		uv, _ := v.(string)
		return f(uv)
	})
}

// PeerIdSet .
type PeerIdSet struct {
	s Set
}

// Put .
func (ps *PeerIdSet) Put(pid peer.ID) bool {
	return ps.s.Put(pid)
}

// Remove .
func (ps *PeerIdSet) Remove(pid peer.ID) bool {
	return ps.s.Remove(pid)
}

// Exist .
func (ps *PeerIdSet) Exist(pid peer.ID) bool {
	return ps.s.Exist(pid)
}

// Size .
func (ps *PeerIdSet) Size() int {
	return ps.s.Size()
}

// Range .
func (ps *PeerIdSet) Range(f func(pid peer.ID) bool) {
	ps.s.Range(func(v interface{}) bool {
		uv, _ := v.(peer.ID)
		return f(uv)
	})
}

// ConnSet .
type ConnSet struct {
	s Set
}

// Put .
func (cs *ConnSet) Put(conn network.Conn) bool {
	return cs.s.Put(conn)
}

// Remove .
func (cs *ConnSet) Remove(conn network.Conn) bool {
	return cs.s.Remove(conn)
}

// Exist .
func (cs *ConnSet) Exist(conn network.Conn) bool {
	if cs == nil {
		return false
	}
	return cs.s.Exist(conn)
}

// Size .
func (cs *ConnSet) Size() int {
	return cs.s.Size()
}

// Range .
func (cs *ConnSet) Range(f func(conn network.Conn) bool) {
	cs.s.Range(func(v interface{}) bool {
		uv, _ := v.(network.Conn)
		return f(uv)
	})
}
