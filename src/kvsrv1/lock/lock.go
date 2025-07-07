package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck        kvtest.IKVClerk
	lockKey   string
	lockValue string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lockKey: l, lockValue: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, getErr := lk.ck.Get(lk.lockKey)
		// try acquire the lock if there's no lock or the lock already be released
		if getErr == rpc.ErrNoKey || value == "" {
			for {
				putErr := lk.ck.Put(lk.lockKey, lk.lockValue, version)
				if putErr == rpc.OK {
					return
				}
				if putErr == rpc.ErrVersion {
					break
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, getErr := lk.ck.Get(lk.lockKey)
		// if lock does not exist or already released or it is not my lock, return
		if getErr == rpc.ErrNoKey || value == "" || value != lk.lockValue {
			return
		}
		for {
			// try to release the lock
			putErr := lk.ck.Put(lk.lockKey, "", version)
			if putErr == rpc.OK {
				return
			}
			if putErr == rpc.ErrVersion {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
