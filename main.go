package main

import (
	"sort"
	"strings"

	"github.com/alecthomas/kong"
	"gitlab.com/tozd/go/errors"
	"gitlab.com/tozd/go/zerolog"

	"gitlab.com/tozd/go/cli"
)

var engines = []Engine{
	&Badger{},
	&Bbolt{},
	&Bitcask{},
	&Buntdb{},
	&Immudb{},
	&FS{},
	&Nutsdb{},
	&Pebble{},
	&Postgresql{},
	&PostgresqlLO{},
	&Sqlite{},
}

var enginesMap = map[string]Engine{}

//nolint:lll
type App struct {
	zerolog.LoggingConfig

	Benchmark Benchmark `cmd:"" help:"Run the benchmark."`
	Plot      Plot      `cmd:"" help:"Plot results from logs."`
}

func main() {
	names := []string{}
	for _, engine := range engines {
		name := engine.Name()
		names = append(names, name)
		enginesMap[name] = engine
	}
	sort.Strings(names)
	var app App
	cli.Run(&app, kong.Vars{
		"engines": strings.Join(names, ","),
	}, func(ctx *kong.Context) errors.E {
		return errors.WithStack(ctx.Run(app.Logger))
	})
}
