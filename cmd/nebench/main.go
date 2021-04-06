package main

import (
	"fmt"
	"os"
	"path/filepath"
	"bufio"
	"sort"
	"path"
	"strconv"
	"encoding/json"
	"strings"
	"time"
	// "runtime/pprof"

	"github.com/pkg/errors"
	"github.com/oklog/run"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"

	"github.com/prometheus/prometheus/storage/tsdb"
	"github.com/prometheus/prometheus/tsdb/labels"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func readNodeExporterLabels(filename string) ([]labels.Labels, error) {
	var lbls []labels.Labels
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() && len(lbls) < 584 {
		m := make(map[string]string)
		err = json.Unmarshal([]byte(scanner.Text()), &m)
		delete(m, "instance")
		lset := labels.FromMap(m)
		sort.Sort(lset)
		lbls = append(lbls, lset)
	}

	file.Close()

	return lbls, nil
}

type timeSeries struct {
	labels labels.Labels
	data []float64
}

type nodeData []timeSeries

func readNodeExporterData(dataPath string, targets int, lbls []labels.Labels) ([]nodeData, error) {
	nodes := []nodeData{}

	for i := 0; i < targets; i++ {
		node := nodeData{}
		for _, lset := range lbls {
			nodeLbls := append(lset, labels.Label{Name: "instance", Value: fmt.Sprintf("pc9%04d:9100", i)})
			node = append(node, timeSeries{labels: nodeLbls, data: []float64{}})
		}

		file, _ := os.Open(path.Join(dataPath, "data" + strconv.Itoa(i)))

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			s := scanner.Text()
			data := strings.Split(s, ",")
			for i := 0; i < len(data) && i < len(lbls); i++ {
				f, _ := strconv.ParseFloat(data[i], 64)
				node[i].data = append(node[i].data, f)
			}
		}

		nodes = append(nodes, node)


		file.Close()
	}

	return nodes, nil
}

func main() {
	cfg := struct {
		localStoragePath    string
		tsdb                tsdb.Options

		promlogConfig promlog.Config

		nodeExporterDataPath string
		nodeExporterSeriesFile string
	}{
		promlogConfig: promlog.Config{},
	}

	a := kingpin.New(filepath.Base(os.Args[0]), "The Prometheus monitoring server")

	a.HelpFlag.Short('h')

	a.Flag("storage.tsdb.path", "Base path for metrics storage.").
		Default("data/").StringVar(&cfg.localStoragePath)

	a.Flag("storage.tsdb.min-block-duration", "Minimum duration of a data block before being persisted. For use in testing.").
		Hidden().Default("2h").SetValue(&cfg.tsdb.MinBlockDuration)

	a.Flag("storage.tsdb.max-block-duration",
		"Maximum duration compacted blocks may span. For use in testing. (Defaults to 10% of the retention period.)").
		Hidden().PlaceHolder("<duration>").SetValue(&cfg.tsdb.MaxBlockDuration)

	a.Flag("storage.tsdb.wal-segment-size",
		"Size at which to split the tsdb WAL segment files. Example: 100MB").
		Hidden().PlaceHolder("<bytes>").BytesVar(&cfg.tsdb.WALSegmentSize)

	a.Flag("node-exporter.data.path", "Data path for node exporter benchmark.").
		Default("../data/node_exporter/").StringVar(&cfg.nodeExporterDataPath)
	a.Flag("node-exporter.series.file", "Series file for node exporter benchmark.").
		Default("timeseries0.json").StringVar(&cfg.nodeExporterSeriesFile)

	promlogflag.AddFlags(a, &cfg.promlogConfig)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	neSeriesFile := path.Join(cfg.nodeExporterDataPath, cfg.nodeExporterSeriesFile)
	lbls, err := readNodeExporterLabels(neSeriesFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "opening series file failed"))
		os.Exit(2)
	}

	nodeData, _ := readNodeExporterData(cfg.nodeExporterDataPath, 70, lbls)

	logger := promlog.New(&cfg.promlogConfig)

	db, err := tsdb.Open(
		cfg.localStoragePath,
		log.With(logger, "component", "tsdb"),
		prometheus.DefaultRegisterer,
		&cfg.tsdb,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "opening storage failed"))
		os.Exit(2)
	}

	// app := db.Appender()
	// app.Add(labels.FromMap(map[string]string{"a": "1", "b": "1", "c": "1"}), 10000, 100.0)
	// app.Add(labels.FromMap(map[string]string{"a": "1", "b": "1", "c": "1"}), 10001, 200.0)
	// app.Commit()

	// querier, _ := db.Querier(10000, 20000)
	// ss, _ := querier.Select(labels.NewEqualMatcher("a", "1"))

	// for ss.Next() {
	//	fmt.Printf("%v\n", ss.At().Labels())
	//	it := ss.At().Iterator()
	//	for it.Next() {
	//		t, v := it.At()

	//		fmt.Printf("%d %f\n", t, v)
	//	}
	// }

	var g run.Group
	{
		cancel := make(chan struct{})
		profile := make(chan struct{})

		g.Add(func() error {
			for t := 0; t < 2160; t++ {
				select {
				case <-cancel:
					return nil
				case <-time.After(5 * time.Second):
					break
				}

				app := db.Appender()
				timestamp := time.Now().UnixNano() / int64(time.Millisecond)

				for _, node := range nodeData {
					for _, ts := range node {
						app.Add(ts.labels, timestamp, ts.data[t])
					}
				}

				if err := app.Commit(); err != nil {
					return err
				}

				if t == 1 {
					close(profile)
				}
			}
			return nil
		},
			func(err error) {
				close(cancel)
			})

		g.Add(func() error {
			f, _ := os.Create("/tmp/prometheus.csv")
			defer f.Close()

			f.WriteString("timestamp,cpu,cpu_count,mem,mem_count,transmit,transmit_count\n")
			for {
				select {
				case <-cancel:
					return nil
				case <-time.After(10 * time.Second):
					break
				}

				now := time.Now()
				then := now.Add(time.Duration(-5) * time.Minute)

				q, _ := db.Querier(then.UnixNano() / int64(time.Millisecond), now.UnixNano() / int64(time.Millisecond))

				f.WriteString(fmt.Sprintf("%d", now.UnixNano() / int64(time.Millisecond)))
				{
					t1 := time.Now()
					count := 0
					ss, _ := q.Select(labels.NewEqualMatcher("__name__", "node_cpu_seconds_total"))
					for ss.Next() {
						ss.At().Labels()
						count += 1
					}
					t2:= time.Now()
					f.WriteString(fmt.Sprintf(",%d,%d", t2.Sub(t1).Microseconds(), count))
				}
				{
					t1 := time.Now()
					count := 0
					ss, _ := q.Select(labels.NewEqualMatcher("__name__", "node_memory_MemAvailable_bytes"))
					for ss.Next() {
						ss.At().Labels()
						count += 1
					}
					t2:= time.Now()
					f.WriteString(fmt.Sprintf(",%d,%d", t2.Sub(t1).Microseconds(), count))
				}
				{
					t1 := time.Now()
					count := 0
					ss, _ := q.Select(labels.NewEqualMatcher("__name__", "node_network_transmit_bytes_total"))
					for ss.Next() {
						ss.At().Labels()
						count += 1
					}
					t2:= time.Now()
					f.WriteString(fmt.Sprintf(",%d,%d", t2.Sub(t1).Microseconds(), count))
				}

				f.WriteString("\n")
			}

			return nil
		},
			func(err error) {
			})

		// g.Add(func() error {
		//	f, _ := os.Create("/tmp/cpu.prof")
		//	defer f.Close()

		//	<-profile

		//	if err := pprof.StartCPUProfile(f); err != nil {
		//		os.Exit(2)
		//	}
		//	defer pprof.StopCPUProfile()

		//	for i := 0; i < 500; i++ {
		//		now := time.Now()
		//		then := now.Add(time.Duration(-5) * time.Minute)

		//		q, _ := db.Querier(then.UnixNano() / int64(time.Millisecond), now.UnixNano() / int64(time.Millisecond))

		//		{
		//			count := 0
		//			ss, _ := q.Select(labels.NewEqualMatcher("__name__", "node_cpu_seconds_total"))
		//			for ss.Next() {
		//				ss.At().Labels()
		//				count += 1
		//			}
		//		}
		//		{
		//			count := 0
		//			ss, _ := q.Select(labels.NewEqualMatcher("__name__", "node_memory_MemAvailable_bytes"))
		//			for ss.Next() {
		//				ss.At().Labels()
		//				count += 1
		//			}
		//		}
		//		{
		//			count := 0
		//			ss, _ := q.Select(labels.NewEqualMatcher("__name__", "node_network_transmit_bytes_total"))
		//			for ss.Next() {
		//				ss.At().Labels()
		//				count += 1
		//			}
		//		}
		//	}

		//	return nil
		// },
		//	func(err error) {
		//	})
	}

	if err := g.Run(); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
}
