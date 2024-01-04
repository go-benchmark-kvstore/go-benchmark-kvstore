package main

import (
	"io"
	"os"

	"gitlab.com/tozd/go/errors"
)

type Engine interface {
	Name() string
	Init(app *App) errors.E
	Sync() errors.E
	Close() errors.E
	Put(key, value []byte) errors.E
	Get(key []byte) (io.ReadSeekCloser, errors.E)
}

func isEmpty(dir string) bool {
	f, err := os.Open(dir)
	if err != nil {
		// Directory does not exist or cannot be opened.
		// We see that as empty.
		return true
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	return err == io.EOF
}

func sizeFunc(reader io.ReadSeeker) (int64, errors.E) {
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return size, nil
}

// consumerReader simulates http.ServeContent.
func consumerReader(reader io.ReadSeeker) errors.E {
	_, errE := sizeFunc(reader)
	if errE != nil {
		return errE
	}
	_, err := io.Copy(io.Discard, reader)
	return errors.WithStack(err)
}

func RunBenchmark(app *App, engine Engine) errors.E {
	errE := engine.Init(app)
	if errE != nil {
		return errE
	}
	defer engine.Close()

	return nil
}
