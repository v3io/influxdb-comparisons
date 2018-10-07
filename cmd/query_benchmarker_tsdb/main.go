package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	tsdbConfig "github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Program option vars:
var (
	limit          int64
	workers        int
	serverPath     string
	dbPath         string
	configFilePath string
	file           string
	printInterval  uint64
	burnIn         uint64
)

var (
	sourceReader *os.File
	queryPool    sync.Pool
	workersGroup sync.WaitGroup
	statGroup    sync.WaitGroup
	statPool     sync.Pool
	statChan     chan *Stat
	queryChan    chan *TsdbQuery
	tsdbAdapter  *tsdb.V3ioAdapter
	v3ioConf     *tsdbConfig.V3ioConfig
)

type statsMap map[string]*StatGroup

const allQueriesLabel = "all queries"

func init() {
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.StringVar(&serverPath, "server", "", "V3IO Service URL - username:password@ip:port/container")
	flag.StringVar(&dbPath, "table-path", "", "sub path for the TSDB, inside the container")
	flag.StringVar(&configFilePath, "config", "", "path to yaml config file")
	flag.StringVar(&file, "file", "", "Input file")
	flag.Int64Var(&limit, "limit", -1, "Limit the number of queries to send.")
	flag.Uint64Var(&printInterval, "print-interval", 0, "Print timing stats to stderr after this many queries (0 to disable)")

	flag.Parse()

	if file != "" {
		if f, err := os.Open(file); err == nil {
			sourceReader = f
		} else {
			log.Fatalf("Error opening %s: %v\n", file, err)
		}
	}
	if sourceReader == nil {
		sourceReader = os.Stdin
	}

	v3ioConf, err := tsdbConfig.GetOrLoadFromFile(configFilePath)
	if err != nil {
		// if we couldn't load the file and its not the default
		if configFilePath != "" {
			log.Fatal(err, "failed to load config from file "+configFilePath)
		}
		v3ioConf = &tsdbConfig.V3ioConfig{} // initialize struct, will try and set it from individual flags
	}
	if serverPath != "" {
		// read username and password
		if i := strings.LastIndex(serverPath, "@"); i > 0 {
			v3ioConf.Username = serverPath[0:i]
			serverPath = serverPath[i+1:]
			if userpass := strings.Split(v3ioConf.Username, ":"); len(userpass) > 1 {
				v3ioConf.Username = userpass[0]
				v3ioConf.Password = userpass[1]
			}
		}

		slash := strings.LastIndex(serverPath, "/")
		if slash == -1 || len(serverPath) <= slash+1 {
			log.Fatal("missing container name in V3IO URL")
		}
		v3ioConf.WebApiEndpoint = serverPath[0:slash]
		v3ioConf.Container = serverPath[slash+1:]
	}
	if dbPath != "" {
		v3ioConf.TablePath = dbPath
	}

	tsdbAdapter, err = tsdb.NewV3ioAdapter(v3ioConf, nil, nil)
	if err != nil {
		log.Fatal("failed to create TSDB adapter", err)
	}
}

func main() {
	// Make pools to minimize heap usage:
	queryPool = sync.Pool{
		New: func() interface{} {
			return &TsdbQuery{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				MetricName:       make([]byte, 0, 1024),
				AggregationType:  make([]byte, 0, 1024),
				Filter:           make([]byte, 0, 1024),
			}
		},
	}
	statPool = sync.Pool{
		New: func() interface{} {
			return &Stat{
				Label: make([]byte, 0, 1024),
				Value: 0.0,
			}
		},
	}

	statChan = make(chan *Stat, workers)
	queryChan = make(chan *TsdbQuery, workers)

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processQueries()
	}

	statGroup.Add(1)
	go processStats()

	wallStart := time.Now()
	scan()
	close(queryChan)

	workersGroup.Wait()

	close(statChan)
	statGroup.Wait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err := fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}
}

func scan() {
	input := bufio.NewReaderSize(sourceReader, 1<<20)
	dec := gob.NewDecoder(input)

	fmt.Printf("start scanning - limit: %v\n", limit)
	for n := int64(0); limit < 0 || n <= limit; n++ {
		q := queryPool.Get().(*TsdbQuery)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		queryChan <- q
	}
}

func processQueries() {
	i := 0
	for q := range queryChan {
		i++
		querier, err := tsdbAdapter.Querier(nil, q.TimeStart, q.TimeEnd)
		if err != nil {
			log.Fatalf("error while creating querier: %v", err.Error())
		}
		before := time.Now()
		querySet, err := querier.Select(string(q.MetricName), string(q.AggregationType), q.Step, string(q.Filter))
		if err != nil {
			log.Fatalf("error while querying for data: %v", err.Error())
		}

		for querySet.Next() {
			iter := querySet.At().Iterator()
			for iter.Next() {
			}
		}
		after := time.Now()
		queryPool.Put(q)

		stat := statPool.Get().(*Stat)
		stat.Init(q.HumanLabel, float64(after.UnixNano()-before.UnixNano())/1000000)
		statChan <- stat

		if querySet.Err() != nil {
			log.Fatalf("error while processing query results: %v", querySet.Err().Error())
		}

	}
	fmt.Printf("finish %v queries\n", i)
	workersGroup.Done()
}

// processStats collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func processStats() {

	statMapping := statsMap{
		allQueriesLabel: &StatGroup{},
	}

	i := uint64(0)
	for stat := range statChan {
		if _, ok := statMapping[string(stat.Label)]; !ok {
			statMapping[string(stat.Label)] = &StatGroup{}
		}

		statMapping[allQueriesLabel].Push(stat.Value)
		statMapping[string(stat.Label)].Push(stat.Value)

		statPool.Put(stat)

		i++

		// print stats to stderr (if printInterval is greater than zero):
		if printInterval > 0 && i > 0 && i%printInterval == 0 && (int64(i) < limit || limit < 0) {
			_, err := fmt.Fprintf(os.Stderr, "after %d queries with %d workers:\n", i-burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
			fprintStats(os.Stderr, statMapping)
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// the final stats output goes to stdout:
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i-burnIn, workers)
	if err != nil {
		log.Fatal(err)
	}
	fprintStats(os.Stdout, statMapping)
	statGroup.Done()
}

// fprintStats pretty-prints stats to the given writer.
func fprintStats(w io.Writer, statGroups statsMap) {
	maxKeyLength := 0
	keys := make([]string, 0, len(statGroups))
	for k := range statGroups {
		if len(k) > maxKeyLength {
			maxKeyLength = len(k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := statGroups[k]
		minRate := 1e3 / v.Min
		meanRate := 1e3 / v.Mean
		maxRate := 1e3 / v.Max
		paddedKey := fmt.Sprintf("%s", k)
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}
		_, err := fmt.Fprintf(w, "%s : min: %8.2fms (%7.2f/sec), mean: %8.2fms (%7.2f/sec), max: %7.2fms (%6.2f/sec), count: %8d, sum: %5.1fsec \n", paddedKey, v.Min, minRate, v.Mean, meanRate, v.Max, maxRate, v.Count, v.Sum/1e3)
		if err != nil {
			log.Fatal(err)
		}
	}

}
