package main

import (
	"sort"
	"strings"

	"github.com/alecthomas/kong"
	"gitlab.com/tozd/go/errors"
	"gitlab.com/tozd/go/zerolog"

	"gitlab.com/tozd/go/cli"
)

//nolint:exhaustruct,gochecknoglobals
var engines = []Engine{
	&Badger{},
	&Bbolt{},
	&Bitcask{},
	&Buntdb{},
	&Immudb{},
	&FS{},
	&FSClone{},
	&Nutsdb{},
	&Pebble{},
	&Postgres{},
	&PostgresLO{},
	&Sqlite{},
}

var enginesMap = map[string]Engine{} //nolint:gochecknoglobals

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
