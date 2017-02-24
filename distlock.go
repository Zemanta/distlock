package distlock

import (
	"errors"
	"log"
	"os"
)

var Logger = log.New(os.Stderr, "", log.LstdFlags)

var ErrLocked = errors.New("Locked")

type Releaser interface {
	Release() error
}

type Locker interface {
	Lock(string) (Releaser, error)
	LockWait(string) (Releaser, error)
}
