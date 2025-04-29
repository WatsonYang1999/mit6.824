package lock

import (
	"fmt"
	"strings"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	locker  string
	owner   string
	version rpc.Tversion
	// You may add code here
}

// type IKVClerk interface {
// 	Get(string) (string, rpc.Tversion, rpc.Err)
// 	Put(string, string, rpc.Tversion) rpc.Err
// }

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:      ck,
		locker:  l,
		owner: kvtest.RandValue(8),
		version: 1,
	}
	ck.Put(l,"UNLOCKED",0)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		status, ver, ok := lk.ck.Get(lk.locker)
		fmt.Println("Get Lock Status:",status, ver, ok)
		if ok == rpc.ErrNoKey {
			panic("Invalid Key")
		}
		if strings.HasPrefix(status, "LOCKED")  && strings.TrimPrefix(status, "LOCKED") != lk.owner{ // other client using the lock
			// update the version and wait
			fmt.Println("status is LOCKED")
			time.Sleep(time.Second)
			continue
		} else if strings.HasPrefix(status, "LOCKED")  && strings.TrimPrefix(status, "LOCKED") == lk.owner{
			// due to unreliable network, this client already get the lock
			break
		}	
		lk.version = ver
		ok = lk.ck.Put(lk.locker, "LOCKED" + lk.owner, lk.version)
		fmt.Println(lk.locker, lk.version, ok)
		if ok == rpc.ErrNoKey {
			panic("Invalid Key")
		} else if ok == rpc.ErrVersion {
			// other client have already taken the lock
			fmt.Println("Failed to grab the lock")
			time.Sleep(time.Second)
		} else if ok == rpc.ErrMaybe {
			// could be locked by this client or other client, in next loop to check lock owner
			continue
		}else{
			// entering critical section
			fmt.Println("Entering Critical Section")
			break
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	status, ver, _ := lk.ck.Get(lk.locker)

	if strings.TrimPrefix(status, "LOCKED") != lk.owner {
		panic("Trying to release a lock with illegal owner!!!")
	}

	for {
		ok := lk.ck.Put(lk.locker, "UNLOCKED", ver)
		if ok == rpc.OK {
			break
		}
		if ok == rpc.ErrMaybe { // for unreliable cases, get again to check if released
			status, _, _ := lk.ck.Get(lk.locker)
			if status == "UNLOCKED" {
				break
			}
		} else {

		}
	}
}
