package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"slices"

	"github.com/hashicorp/go-metrics"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

type readSeekCloser struct {
	io.ReadSeeker
	close func() error
}

func (r readSeekCloser) Close() error {
	return r.close()
}

type readSeekCloseWriterTo struct {
	io.ReadSeeker
	io.WriterTo
	close func() error
}

func (r readSeekCloseWriterTo) Close() error {
	return r.close()
}

func bytesReadSeekCloser(value []byte, closeFn func() error) io.ReadSeekCloser {
	return readSeekCloser{
		ReadSeeker: bytes.NewReader(value),
		close:      closeFn,
	}
}

func newReadSeekCloser(readSeeker io.ReadSeeker, closeFn func() error) io.ReadSeekCloser {
	if wt, ok := readSeeker.(io.WriterTo); ok {
		return readSeekCloseWriterTo{
			ReadSeeker: readSeeker,
			WriterTo:   wt,
			close:      closeFn,
		}
	}
	return readSeekCloser{
		ReadSeeker: readSeeker,
		close:      closeFn,
	}
}

type metricsEncoder struct {
	Logger zerolog.Logger
}

func (e metricsEncoder) Encode(value interface{}) error {
	if v, ok := value.(metrics.MetricsSummary); ok {
		for _, counter := range v.Counters {
			if slices.Contains([]string{"set", "get"}, counter.Name) {
				e.Logger.Info().Float64("rate", counter.Rate).Int("count", counter.Count).
					Str("timestamp", v.Timestamp).
					Msgf("counter %s", counter.Name)
			}
		}
		for _, sample := range v.Samples {
			if slices.Contains([]string{"set", "get.ready", "get.total", "get.first"}, sample.Name) {
				e.Logger.Info().Float64("min", sample.Min).Float64("max", sample.Max).
					Float64("mean", sample.Mean).Str("timestamp", v.Timestamp).
					Msgf("sample %s", sample.Name)
			}
		}
	}
	return nil
}

func getModuleVersion(path string) (string, errors.E) {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return "", errors.New("build info not available")
	}
	// We first search replaced modules.
	for _, module := range buildInfo.Deps {
		if module.Replace != nil && module.Replace.Path == path {
			return module.Version, nil
		}
	}
	// Then all other modules.
	for _, module := range buildInfo.Deps {
		if module.Path == path {
			return module.Version, nil
		}
	}
	return "", errors.Errorf(`module version not found for "%s"`, path)
}

func getGoCompile() string {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		panic(errors.New("build info not available"))
	}
	return buildInfo.GoVersion
}

func postgresVersion(connString string) (string, errors.E) {
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer conn.Close(context.Background())
	var v1 string
	err = conn.QueryRow(context.Background(), `SHOW server_version`).Scan(&v1)
	if err != nil {
		return "", errors.WithStack(err)
	}
	v2, errE := getModuleVersion("github.com/jackc/pgx/v5")
	if errE != nil {
		return "", errE
	}
	return fmt.Sprintf("%s/%s", v1, v2), nil
}
