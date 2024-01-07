package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/google/uuid"
	"github.com/hashicorp/go-metrics"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
	"golang.org/x/sync/errgroup"
)

const (
	dataSeed = 42
)

var (
	keySeed = uuid.MustParse("9dd5f08a-74f2-4d91-a6f9-cd72cfe2e516")
)

type Benchmark struct {
	Engine     string            `arg:"" enum:"${engines}" required:"" help:"Engine to use. Possible: ${engines}."`
	Data       string            `short:"d" default:"/tmp/data" placeholder:"DIR" help:"Data directory to use. Default: ${default}."`
	Postgresql string            `short:"P" default:"postgres://test:test@localhost:5432" placeholder:"URI" help:"Address of running PostgreSQL. Data directory should point to its disk storage. Default: ${default}."`
	Readers    int               `short:"r" default:"1" help:"Number of concurrent readers. Default: ${default}." placeholder:"INT"`
	Writers    int               `short:"w" default:"1" help:"Number of concurrent writers. Default: ${default}." placeholder:"INT"`
	Size       datasize.ByteSize `short:"s" default:"1MB" help:"Size of values to use. Default: ${default}." placeholder:"SIZE"`
	Vary       bool              `short:"v" default:"false" help:"Vary the size of values up to the size limit. Default: ${default}." placeholder:"BOOL"`
	Time       time.Duration     `short:"t" default:"20m" help:"For how long to run the benchmark. Default: ${default}." placeholder:"DURATION"`
}

func (b *Benchmark) Run(logger zerolog.Logger) errors.E {
	engine := enginesMap[b.Engine]
	logger.Info().Str("engine", engine.Name()).Int("writers", b.Writers).Int("readers", b.Readers).Str("data", b.Data).Msg("running")

	errE := engine.Init(b, logger)
	if errE != nil {
		return errE
	}
	defer func() {
		errE = errors.Join(errE, engine.Close())
	}()

	errE = testEngine(engine)
	if errE != nil {
		return errE
	}

	inm := metrics.NewInmemSink(10*time.Second, b.Time+10*time.Second)
	cfg := metrics.DefaultConfig("benchmark")
	cfg.EnableHostname = false
	cfg.EnableServiceLabel = true
	mtr, err := metrics.New(cfg, inm)
	if err != nil {
		return errors.WithStack(err)
	}
	defer mtr.Shutdown()

	r := rand.New(rand.NewSource(dataSeed))
	writeData := make([]byte, 2*b.Size)
	_, err = r.Read(writeData)
	if err != nil {
		return errors.WithStack(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.Time)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		inm.Stream(ctx, metricsEncoder{logger})
		return nil
	})

	for i := 0; i < b.Writers; i++ {
		i := i

		g.Go(func() error {
			return writeEngine(ctx, mtr, engine, writeData, int(b.Size), b.Vary, uint64(i), uint64(b.Writers))
		})
	}

	return errors.WithStack(g.Wait())
}

type Engine interface {
	Name() string
	Init(benchmark *Benchmark, logger zerolog.Logger) errors.E
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
func consumerReader(devNull *os.File, reader io.ReadSeeker) errors.E {
	_, errE := sizeFunc(reader)
	if errE != nil {
		return errE
	}
	// We do not use io.Discard but /dev/null file to simulate realistic copying out of the process.
	_, err := io.Copy(devNull, reader)
	return errors.WithStack(err)
}

// Some basic operations to test the engine.
func testEngine(engine Engine) errors.E {
	_, errE := engine.Get([]byte("does not exist"))
	if errE == nil {
		return errors.New("expected error")
	}

	errE = engine.Put([]byte("key"), []byte("value"))
	if errE != nil {
		return errE
	}

	valueReader, errE := engine.Get([]byte("key"))
	if errE != nil {
		return errE
	}
	value, err := io.ReadAll(valueReader)
	err2 := valueReader.Close()
	if err != nil || err2 != nil {
		return errors.Join(err, err2)
	}

	if exp := []byte("value"); !bytes.Equal(value, exp) {
		return errors.Errorf(`expected "%v", got "%v"`, exp, value)
	}

	errE = engine.Put([]byte("key"), []byte("foobar"))
	if errE != nil {
		return errE
	}

	valueReader, errE = engine.Get([]byte("key"))
	if errE != nil {
		return errE
	}
	value, err = io.ReadAll(valueReader)
	err2 = valueReader.Close()
	if err != nil || err2 != nil {
		return errors.Join(err, err2)
	}

	if exp := []byte("foobar"); !bytes.Equal(value, exp) {
		return errors.Errorf(`expected "%v", got "%v"`, exp, value)
	}

	errE = engine.Sync()
	if errE != nil {
		return errE
	}

	return nil
}

func writeEngine(ctx context.Context, mtr *metrics.Metrics, engine Engine, writeData []byte, size int, vary bool, offset uint64, total uint64) errors.E {
	iBytes := make([]byte, 8)
	r := rand.New(rand.NewSource(int64(offset)))
	for i := uint64(0); ctx.Err() == nil; i++ {
		binary.BigEndian.PutUint64(iBytes, i*total+offset)
		key := uuid.NewSHA1(keySeed, iBytes)
		// writeData has length 2*size, so offset can be on interval [0, size].
		dataOffset := r.Intn(size + 1)
		var dataSize int
		if vary {
			// We want size to be on interval [1, size].
			dataSize = r.Intn(size) + 1
		} else {
			dataSize = size
		}
		start := time.Now()
		errE := engine.Put(key[:], writeData[dataOffset:dataOffset+dataSize])
		mtr.MeasureSince([]string{"put"}, start)
		mtr.IncrCounter([]string{"put"}, 1)
		if errE != nil {
			return errE
		}
	}
	return nil
}
