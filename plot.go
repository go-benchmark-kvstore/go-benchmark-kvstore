package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

type Plot struct {
	Files []string `arg:"" required:"" help:"JSON log file(s) to use." name:"file" type:"existingfile"`
}

type logEntry struct {
	Level   string `json:"level"`
	Message string `json:"message"`
	Time    string `json:"time"`

	Engine  string `json:"engine"`
	Writers int    `json:"writers"`
	Readers int    `json:"readers"`
	Size    int    `json:"size"`
	Vary    bool   `json:"vary"`

	Timestamp string `json:"timestamp"`

	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	Min    float64 `json:"min"`
	Stddev float64 `json:"stddev"`

	Count int     `json:"count"`
	Rate  float64 `json:"rate"`
}

type plotConfig struct {
	Writers int
	Readers int
	Size    int
	Vary    bool
}

type plotMeasurements struct {
	Engine string
	Config plotConfig

	Timestamps []time.Duration

	Data map[string][]float64
}

func makeLineData(data []float64) []opts.LineData {
	result := make([]opts.LineData, len(data))
	for i, v := range data {
		result[i].Value = v
	}
	return result
}

func (p *Plot) Run(logger zerolog.Logger) errors.E {
	data := map[plotConfig][]*plotMeasurements{}

	for _, path := range p.Files {
		measurements, errE := p.processFile(path)
		if errE != nil {
			return errE
		}
		data[measurements.Config] = append(data[measurements.Config], measurements)
	}

	for config, allMeasurements := range data {
		for _, name := range []string{"get rate"} {
			errE := p.renderPlot(config, name, allMeasurements)
			if errE != nil {
				return errE
			}
		}
	}

	return nil
}

func (p *Plot) processFile(path string) (*plotMeasurements, errors.E) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	measurements := &plotMeasurements{
		Data: make(map[string][]float64),
	}
	var start time.Time

	for {
		var entry logEntry
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, errors.WithStack(err)
		}

		if entry.Timestamp != "" {
			timestamp, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", entry.Timestamp)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			if start.IsZero() {
				start = timestamp
				measurements.Timestamps = append(measurements.Timestamps, 0)
			} else {
				sinceStart := timestamp.Sub(start)
				if measurements.Timestamps[len(measurements.Timestamps)-1] != sinceStart {
					measurements.Timestamps = append(measurements.Timestamps, sinceStart)
				}
			}
		}

		switch entry.Message {
		case "running":
			measurements.Engine = entry.Engine
			measurements.Config.Writers = entry.Writers
			measurements.Config.Readers = entry.Readers
			measurements.Config.Size = entry.Size
			measurements.Config.Vary = entry.Vary
		case "counter get":
			measurements.Data["get rate"] = append(measurements.Data["get rate"], entry.Rate)
		case "counter put":
			measurements.Data["put rate"] = append(measurements.Data["put rate"], entry.Rate)
		case "sample get.ready":
			measurements.Data["get ready min"] = append(measurements.Data["get ready min"], entry.Min)
			measurements.Data["get ready max"] = append(measurements.Data["get ready max"], entry.Max)
			measurements.Data["get ready mean"] = append(measurements.Data["get ready mean"], entry.Mean)
			measurements.Data["get ready stddev"] = append(measurements.Data["get ready stddev"], entry.Stddev)
		case "sample get.first":
			measurements.Data["get first min"] = append(measurements.Data["get first min"], entry.Min)
			measurements.Data["get first max"] = append(measurements.Data["get first max"], entry.Max)
			measurements.Data["get first mean"] = append(measurements.Data["get first mean"], entry.Mean)
			measurements.Data["get first stddev"] = append(measurements.Data["get first stddev"], entry.Stddev)
		case "sample get.total":
			measurements.Data["get total min"] = append(measurements.Data["get total min"], entry.Min)
			measurements.Data["get total max"] = append(measurements.Data["get total max"], entry.Max)
			measurements.Data["get total mean"] = append(measurements.Data["get total mean"], entry.Mean)
			measurements.Data["get total stddev"] = append(measurements.Data["get total stddev"], entry.Stddev)
		case "sample put":
			measurements.Data["put min"] = append(measurements.Data["put min"], entry.Min)
			measurements.Data["put max"] = append(measurements.Data["put max"], entry.Max)
			measurements.Data["put mean"] = append(measurements.Data["put mean"], entry.Mean)
			measurements.Data["put stddev"] = append(measurements.Data["put stddev"], entry.Stddev)
		}
	}

	length := len(measurements.Timestamps)
	for _, values := range measurements.Data {
		if len(values) < length {
			length = len(values)
		}
	}

	measurements.Timestamps = measurements.Timestamps[:length]
	for name, values := range measurements.Data {
		measurements.Data[name] = values[:length]
	}

	return measurements, nil
}

func (p *Plot) renderPlot(config plotConfig, name string, allMeasurements []*plotMeasurements) errors.E {
	line := charts.NewLine()
	line.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    name,
			Subtitle: fmt.Sprintf("writers=%d readers=%d size=%d vary=%t", config.Writers, config.Readers, config.Size, config.Vary),
		}),
	)
	var timestamps []time.Duration
	for _, measurements := range allMeasurements {
		if len(measurements.Timestamps) > len(timestamps) {
			timestamps = measurements.Timestamps
		}
		line.AddSeries(measurements.Engine, makeLineData(measurements.Data[name]))
	}
	line.SetXAxis(timestamps)
	line.SetSeriesOptions(
		charts.WithLineChartOpts(opts.LineChart{Smooth: true}),
	)
	f, err := os.Create(fmt.Sprintf("result-%s.html", strings.ReplaceAll(name, " ", "_")))
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	return errors.WithStack(line.Render(f))
}
