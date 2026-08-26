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
	ck kvtest.IKVClerk
	// You may add code here

	lockname string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here

	lk.lockname = lockname
	ck.Put(lockname, "", 0)
	lk.id = kvtest.RandValue(8)

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here

	for {
		value, version, _ := lk.ck.Get(lk.lockname)
		if value == lk.id {
			break
		}
		if value != "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err := lk.ck.Put(lk.lockname, lk.id, version)
		if err == rpc.OK {
			break
		} else if err == rpc.ErrMaybe {
			value, _, _ := lk.ck.Get(lk.lockname)
			if value == lk.id {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here

	if value, version, _ := lk.ck.Get(lk.lockname); value == lk.id {
		err := lk.ck.Put(lk.lockname, "", version)

		if err == rpc.ErrMaybe {
			value, version, _ = lk.ck.Get(lk.lockname)
			for value == lk.id {
				value, _, _ = lk.ck.Get(lk.lockname)
				lk.ck.Put(lk.lockname, "", version)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}
