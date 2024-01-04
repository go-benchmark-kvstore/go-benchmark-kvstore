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

//nolint:lll
type App struct {
	zerolog.LoggingConfig

	Engine     string `arg:"" enum:"${engines}" required:"" help:"Engine to use. Possible: ${engines}."`
	Data       string `short:"d" default:"/tmp/data" placeholder:"DIR" help:"Data directory to use. Default: ${default}."`
	Postgresql string `short:"P" default:"postgres://test:test@localhost:5432" placeholder:"HOST:PORT" help:"Address of running PostgreSQL. Data directory should point to its disk storage. Default: ${default}."`
	Readers    int    `short:"r" default:"1" help:"Number of concurrent readers. Default: ${default}." placeholder:"INT"`
	Writers    int    `short:"w" default:"1" help:"Number of concurrent writers. Default: ${default}." placeholder:"INT"`
}

func main() {
	e := map[string]Engine{}
	names := []string{}
	for _, engine := range engines {
		name := strings.ToLower(engine.Name())
		names = append(names, name)
		e[name] = engine
	}
	sort.Strings(names)
	var app App
	cli.Run(&app, kong.Vars{
		"engines": strings.Join(names, ","),
	}, func(ctx *kong.Context) errors.E {
		engine := e[app.Engine]
		app.Logger.Info().Str("engine", engine.Name()).Int("writers", app.Writers).Int("readers", app.Readers).Str("data", app.Data).Msg("running")
		return RunBenchmark(&app, engine)
	})
}
