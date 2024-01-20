package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

const renderErrorBars = `
function (params, api) {
	var xValue = api.value(0);
	var maxPoint = api.coord([xValue, api.value(2)]);
	var minPoint = api.coord([xValue, api.value(3)]);
	var halfWidth = 5;
	var style = api.style({
		stroke: api.visual('color'),
		fill: undefined
	});
	return {
		type: 'group',
		children: [
			{
				type: 'line',
				transition: ['shape'],
				shape: {
					x1: maxPoint[0] - halfWidth,
					y1: maxPoint[1],
					x2: maxPoint[0] + halfWidth,
					y2: maxPoint[1]
				},
				style: style
			},
			{
				type: 'line',
				transition: ['shape'],
				shape: {
					x1: maxPoint[0],
					y1: maxPoint[1],
					x2: minPoint[0],
					y2: minPoint[1]
				},
				style: style
			},
			{
				type: 'line',
				transition: ['shape'],
				shape: {
					x1: minPoint[0] - halfWidth,
					y1: minPoint[1],
					x2: minPoint[0] + halfWidth,
					y2: minPoint[1]
				},
				style: style
			}
		]
	};
}
`

// Needs a better way to show/hide the series.
// See: https://github.com/apache/echarts/issues/15585
// We change encode so that y-axis min/max is computed only based on the data shown.
const toggleErrorBars = `
function () {
	const chart = this.ecModel.scheduler.ecInstance;
	const series = [];
	for (const s of chart.getOption().series) {
		if (s.type === 'custom') {
			if (s.renderItem === null) {
				series.push({renderItem: ` + renderErrorBars + `, encode: {x: [0], y: [2, 3]}});
			} else {
				series.push({renderItem: null, encode: {x: [0], y: [1]}});
			}
		} else {
			series.push({});
		}
	}
	chart.setOption({series: series});
}
`

const tooltipFormatter = `
function (params) {
	if (!params.length) {
		return
	}
	let series = '';
	for (const param of params) {
		if (param.seriesType === 'custom') {
			continue
		}
		series += '<tr style="line-height:1;clear:both">'+
			'<td>'+
				'<span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background-color:'+param.color+';"></span>'+
				'<span style="font-size:14px;color:#666;font-weight:400;margin-left:2px">'+param.seriesName+'</span>'+
			'</td>'+
			'<td style="font-size:14px;color:#666;font-weight:900;text-align:right">'+param.value[1].toFixed(2)+'</td>'+
			(param.value.length > 2 ?
				'<td style="font-size:14px;color:#666;font-weight:400;text-align:right">'+param.value[2].toFixed(2)+'</td>'+
				'<td style="font-size:14px;color:#666;font-weight:400;text-align:right">'+param.value[3].toFixed(2)+'</td>'
			: '')+
		'</tr>'
	}
	return
	'<table style="border-collapse:separate;border-spacing:10px 5px">'+
		'<tr>'+
			'<td style="font-size:14px;color:#666;font-weight:900;line-height:1">'+params[0].axisValue.toFixed(0)+'</td>'+
			'<td style="font-size:14px;color:#666;font-weight:400;line-height:1;text-align:center">mean</td>'+
			(params[0].value.length > 2 ?
				'<td style="font-size:14px;color:#666;font-weight:400;line-height:1;text-align:center">min</td>'+
				'<td style="font-size:14px;color:#666;font-weight:400;line-height:1;text-align:center">max</td>'
			: '')+
		'</tr>'+
		series+
	'</table>';
}
`

//nolint:lll
type Report struct {
	Files  []string `arg:""                                                                  help:"JSON log file(s) to use."                                name:"file"                    required:""           type:"existingfile"`
	Output string   `       default:"index.html"                                             help:"Write the report to this file. Default: ${default}."             placeholder:"FILE"             short:"O" type:"path"`
	Assets string   `       default:"https://go-echarts.github.io/go-echarts-assets/assets/" help:"Location of go-echarts assets. Default: ${default}."                            placeholder:"URL"`
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
	Threads int    `json:"threads"`

	Timestamp string `json:"timestamp"`

	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
	Min  float64 `json:"min"`

	Count int     `json:"count"`
	Rate  float64 `json:"rate"`
}

type plotConfig struct {
	Writers int
	Readers int
	Size    int
	Vary    bool
	Threads int
}

type plotMeasurements struct {
	Engine string
	Config plotConfig

	Timestamps []time.Duration

	Data map[string][][]float64
}

func makeLineData(timestamps []time.Duration, data [][]float64) []opts.LineData {
	result := make([]opts.LineData, len(data))
	for i, values := range data {
		value := []interface{}{timestamps[i] / dataIntervalUnit}
		for _, v := range values {
			value = append(value, v)
		}
		result[i].Value = value
	}
	return result
}

func (p *Report) Run(_ zerolog.Logger) errors.E {
	data := map[plotConfig][]*plotMeasurements{}

	for _, path := range p.Files {
		measurements, errE := p.processFile(path)
		if errE != nil {
			return errE
		}
		data[measurements.Config] = append(data[measurements.Config], measurements)
	}

	return p.renderData(data)
}

func (p *Report) processFile(path string) (*plotMeasurements, errors.E) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	measurements := &plotMeasurements{
		Data: make(map[string][][]float64),
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
			if measurements.Engine != "" {
				return nil, errors.New(`duplicate "running" message in logs`)
			}
			measurements.Engine = entry.Engine
			measurements.Config.Writers = entry.Writers
			measurements.Config.Readers = entry.Readers
			measurements.Config.Size = entry.Size
			measurements.Config.Vary = entry.Vary
			measurements.Config.Threads = entry.Threads
		case "counter get":
			measurements.Data["get rate"] = append(measurements.Data["get rate"], []float64{entry.Rate})
		case "counter set":
			measurements.Data["set rate"] = append(measurements.Data["set rate"], []float64{entry.Rate})
		case "sample get.ready":
			measurements.Data["get ready"] = append(measurements.Data["get ready"], []float64{entry.Mean, entry.Min, entry.Max})
		case "sample get.first":
			measurements.Data["get first"] = append(measurements.Data["get first"], []float64{entry.Mean, entry.Min, entry.Max})
		case "sample get.total":
			measurements.Data["get total"] = append(measurements.Data["get total"], []float64{entry.Mean, entry.Min, entry.Max})
		case "sample set":
			measurements.Data["set"] = append(measurements.Data["set"], []float64{entry.Mean, entry.Min, entry.Max})
		}
	}

	if measurements.Engine == "" {
		return nil, errors.New(`missing "running" message in logs`)
	}

	length := len(measurements.Timestamps)
	for _, values := range measurements.Data {
		if len(values) < length {
			length = len(values)
		}
	}

	if length == 0 || len(measurements.Data) == 0 {
		return nil, errors.New("no data")
	}

	measurements.Timestamps = measurements.Timestamps[:length]
	for name, values := range measurements.Data {
		measurements.Data[name] = values[:length]
	}

	return measurements, nil
}

func (p *Report) renderData(data map[plotConfig][]*plotMeasurements) errors.E {
	page := components.NewPage()
	page.SetLayout(components.PageFlexLayout)
	page.PageTitle = "go-benchmark-kvstore report"
	page.AssetsHost = p.Assets

	for config, allMeasurements := range data {
		for _, name := range []string{"get rate", "set rate", "get ready", "get first", "get total", "set"} {
			plot := p.renderPlot(config, name, allMeasurements)
			page.AddCharts(plot)
		}
	}

	f, err := os.Create(p.Output)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	return errors.WithStack(page.Render(f))
}

func (p *Report) renderPlot(config plotConfig, name string, allMeasurements []*plotMeasurements) components.Charter { //nolint:ireturn
	line := charts.NewLine()
	var better string
	if strings.Contains(name, "rate") {
		line.SetGlobalOptions(
			charts.WithYAxisOpts(opts.YAxis{
				Name:         "ops/s",
				NameLocation: "center",
				Type:         "value",
				NameGap:      50,
			}),
		)
		better = "higher is better"
	} else {
		line.SetGlobalOptions(
			charts.WithYAxisOpts(opts.YAxis{
				Name:         "duration (ms)",
				NameLocation: "center",
				Type:         "value",
				NameGap:      50,
			}),
			charts.WithToolboxOpts(opts.Toolbox{
				Show: true,
				Feature: &opts.ToolBoxFeature{
					Restore: &opts.ToolBoxFeatureRestore{
						Show: true,
					},
					UserDefined: map[string]opts.ToolBoxFeatureUserDefined{
						"myErrorBars": {
							Show:    true,
							Title:   "Toggle error bars",
							Icon:    "path://M 11.359041,7.5285047 V 4.5670261 H 2.4746032 v 2.9614786 h 2.9614791 c -0.021137,11.0157323 0,11.0155383 0,20.7303553 H 2.4746032 v 2.961479 H 11.359041 V 28.25886 H 8.397562 c 0.165371,-14.351131 0,0 0,-20.7303553 z M 26.856729,4.3174113 V 1.3559322 h -8.884437 v 2.9614791 h 2.96148 V 22.086287 h -2.96148 v 2.961478 h 8.884437 v -2.961478 h -2.961478 c 0,-17.7688757 0,0 0,-17.7688757 z", //nolint:lll
							OnClick: opts.FuncOpts(toggleErrorBars),
						},
					},
				},
			}),
		)
		better = "lower is better"
	}
	line.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title: name,
			Subtitle: fmt.Sprintf(
				"writers=%d readers=%d size=%s\nvary=%t threads=%d %s",
				config.Writers, config.Readers, datasize.ByteSize(config.Size), config.Vary, config.Threads, better,
			),
		}),
		charts.WithGridOpts(opts.Grid{
			Top:   "75",
			Left:  "8%",
			Right: "2%",
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name:         fmt.Sprintf("duration (%s)", strings.ReplaceAll(dataIntervalUnit.String(), "1", "")),
			NameLocation: "center",
			Type:         "value",
			NameGap:      30,
		}),
		charts.WithLegendOpts(opts.Legend{
			Show:  true,
			Left:  "280",
			Right: "140",
		}),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:    true,
			Trigger: "axis",
			AxisPointer: &opts.AxisPointer{
				Show: true,
			},
			Formatter: opts.FuncOpts(tooltipFormatter),
		}),
	)
	for _, measurements := range allMeasurements {
		data := makeLineData(measurements.Timestamps, measurements.Data[name])
		line.AddSeries(measurements.Engine, data)
		if !strings.Contains(name, "rate") {
			line.AddSeries(measurements.Engine, data, func(s *charts.SingleSeries) {
				s.Name = measurements.Engine
				s.Type = types.ChartCustom
				s.RenderItem = opts.FuncOpts(renderErrorBars)
			}, charts.WithEncodeOpts(opts.Encode{
				X: []int{0},
				Y: []int{2, 3},
			}), charts.WithItemStyleOpts(opts.ItemStyle{
				BorderWidth: 1.5,
			}))
		}
	}
	line.SetSeriesOptions(
		charts.WithLineChartOpts(opts.LineChart{Smooth: true}),
	)
	return line
}
