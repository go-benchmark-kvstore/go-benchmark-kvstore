package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
	"golang.org/x/sys/unix"
)

var _ Engine = (*FSClone)(nil)

type FSClone struct {
	dir string
}

func (e *FSClone) Close() errors.E {
	return nil
}

func (e *FSClone) Sync() errors.E {
	// We sync after every write, so there is nothing to sync here.
	return nil
}

func (e *FSClone) name(key []byte) string {
	return base64.RawURLEncoding.EncodeToString(key)
}

func (e *FSClone) Get(key []byte) (_ io.ReadSeekCloser, errE errors.E) {
	name := e.name(key)

	f, err := os.Open(path.Join(e.dir, name))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// We can use defer here because f is used only until this function returns.
	defer func() {
		errE = errors.Join(errE, f.Close())
	}()

	fConn, err := f.SyscallConn()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	snapshot, err := os.CreateTemp(e.dir, fmt.Sprintf("%s.temp-*", name))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	snapshotConn, err := snapshot.SyscallConn()
	if err != nil {
		return nil, errors.Join(err, snapshot.Close())
	}

	// We make a file clone of the file (supported on filesystems like Btrfs,
	// XFS, ZFS, APFS and ReFSv2) to create a snapshot of the file f.
	var err2, err3 error
	err = fConn.Control(func(fFd uintptr) {
		err2 = snapshotConn.Control(func(snapshotFd uintptr) {
			// We have to cast file descriptors to int.
			// See: https://github.com/golang/go/issues/64992
			err3 = unix.IoctlFileClone(int(snapshotFd), int(fFd))
		})
	})

	if err != nil || err2 != nil || err3 != nil {
		return nil, errors.Join(err, err2, err3, snapshot.Close())
	}

	return newReadSeekCloser(snapshot, func() error {
		return errors.Join(snapshot.Close(), os.Remove(snapshot.Name()))
	}), nil
}

func (e *FSClone) Init(benchmark *Benchmark, logger zerolog.Logger) errors.E {
	err := os.MkdirAll(benchmark.Data, 0700)
	if err != nil {
		return errors.WithStack(err)
	}
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	e.dir = benchmark.Data
	return nil
}

func (*FSClone) Name() string {
	return "fsclone"
}

func (e *FSClone) Set(key []byte, value []byte) (errE errors.E) {
	name := e.name(key)
	f, err := os.CreateTemp(e.dir, fmt.Sprintf("%s.temp-*", name))
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		errE2 := f.Close()
		var errE3 errors.E
		if errE == nil && errE2 == nil {
			// There was no error, we atomically (on Unix) rename the file to final filename.
			errE3 = errors.WithStack(os.Rename(f.Name(), path.Join(e.dir, name)))
		}
		if errE != nil || errE2 != nil || errE3 != nil {
			// There was an error, we remove the file.
			errE = errors.Join(errE, errE2, errE3, os.Remove(f.Name()))
		}
	}()

	_, err = f.Write(value)
	if err != nil {
		return errors.WithStack(err)
	}

	// To be able to compare between engines, we make all of them sync after every write.
	// This lowers throughput, but it makes relative differences between engines clearer.
	return errors.WithStack(f.Sync())
}
