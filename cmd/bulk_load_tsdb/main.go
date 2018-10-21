package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	tsdbConfig "github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	tsdbUtils "github.com/v3io/v3io-tsdb/pkg/utils"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Program option vars:
var (
	itemLimit      int64
	workers        int
	printInterval  int64
	serverPath     string
	dbPath         string
	configFilePath string
)

// Global vars
var (
	inputDone    chan struct{}
	workersGroup sync.WaitGroup

	tsdbAdapter       *tsdb.V3ioAdapter
	tsdbAppender      tsdb.Appender
	batchTSDBChan     chan *TSDBDataPoint
	batchTSDBChannels []chan *TSDBDataPoint
	v3ioConf          *tsdbConfig.V3ioConfig
)

type TSDBDataPoint struct {
	MetricsName string
	Labels      tsdbUtils.Labels
	Value       float64
	Timestamp   int64
}

func init() {
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.Int64Var(&itemLimit, "item-limit", -1, "Number of parallel requests to make.")
	flag.Int64Var(&printInterval, "print-interval", 1000000, "Number of records after which to print a log")
	flag.StringVar(&serverPath, "server", "", "V3IO Service URL - username:password@ip:port/container")
	flag.StringVar(&dbPath, "table-path", "", "sub path for the TSDB, inside the container")
	flag.StringVar(&configFilePath, "config", "", "path to yaml config file")

	flag.Parse()

	var err error
	v3ioConf, err = tsdbConfig.GetOrLoadFromFile(configFilePath)
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

	fmt.Println("v3io conf", v3ioConf)
	batchTSDBChan = make(chan *TSDBDataPoint, workers)
	batchTSDBChannels = make([]chan *TSDBDataPoint, workers)
	for i := 0; i < workers; i++ {
		batchTSDBChannels[i] = make(chan *TSDBDataPoint, 1)
	}
}

func main() {
	//// Measure performance
	//metricReporter, _ := performance.DefaultReporterInstance()
	//metricReporter.Start()
	//defer metricReporter.Stop()

	createTSDB()

	tsdbAdapter, err := tsdb.NewV3ioAdapter(v3ioConf, nil, nil)
	if err != nil {
		log.Fatalf("failed to create TSDB adapter: %v", err)
	}

	tsdbAppender, err = tsdbAdapter.Appender()
	if err != nil {
		log.Fatalf("failed to create tsdb appender: %v", err)
	}

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processRecords(i)
	}

	start := time.Now()
	itemsRead, bytesRead := scan()
	close(batchTSDBChan)
	for i := 0; i < workers; i++ {
		close(batchTSDBChannels[i])
	}
	workersGroup.Wait()

	end := time.Now()
	took := end.Sub(start)
	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())

	log.Printf("loaded %d items in %fsec with %d workers (mean point rate %f items/sec, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, bytesRate/(1<<20))
}

// scan reads one item at a time from stdin. 1 item = 1 line.
func scan() (int64, int64) {
	var itemsRead, bytesRead int64

	scanner := bufio.NewScanner(bufio.NewReaderSize(os.Stdin, 4*1024*1024))

	for scanner.Scan() {
		if itemsRead == itemLimit {
			break
		}

		line := scanner.Text()

		data, err := stringToTSDBData(line)
		if err != nil {
			log.Fatal(err)
		}
		//		batchTSDBChan <- data
		_, _, hash := data.Labels.GetKey()
		batchTSDBChannels[hash%uint64(workers)] <- data

		itemsRead++
		bytesRead += int64(len(scanner.Bytes()))
		if itemsRead%printInterval == 0 {
			log.Printf("read %v events\n", itemsRead)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	//Closing inputDone signals to the application that we've read everything and can now shut down.
	//close(inputDone)

	return itemsRead, bytesRead
}

func processRecords(number int) {
	var total int64
	var events int

	tsdbAdapter, err := tsdb.NewV3ioAdapter(v3ioConf, nil, nil)
	if err != nil {
		log.Fatalf("failed to create TSDB adapter: %v", err)
	}

	tsdbAppender, err := tsdbAdapter.Appender()
	if err != nil {
		log.Fatalf("failed to create tsdb appender: %v", err)
	}

	for data := range batchTSDBChannels[number] {
		events++
		start := time.Now().UnixNano()
		//log.Printf("labels: %v", data.Labels)
		//log.Printf("time: %v", data.Timestamp)
		//log.Printf("value: %v", data.Value)
		//log.Printf("metric name: %v", data.MetricsName)
		//name, key, hash:=data.Labels.GetKey()
		//log.Printf("name: %v, key: %v, hash: %v", name, key, hash)
		_, err := tsdbAppender.Add(data.Labels, data.Timestamp, data.Value)
		if err != nil {
			log.Fatal(err)
		}

		total += time.Now().UnixNano() - start
		//log.Printf("got ref: %v", ref)
	}

	tsdbAppender.WaitForCompletion(time.Duration(5))
	log.Println("Process time: ", total, "number of events:", events)
	workersGroup.Done()
}

func stringToTSDBData(data string) (*TSDBDataPoint, error) {
	dataParts := strings.Split(data, "#")
	if len(dataParts) != 4 {
		return nil, errors.Errorf("Message is in invalid format: %s", data)
	}
	metricName := dataParts[0]
	labels := strings.Split(dataParts[1], " ")
	labels = append(labels, "__name__", metricName)
	val, err := strconv.ParseFloat(dataParts[2], 64)
	if err != nil {
		return nil, errors.Wrap(err, "can not parse value of data point")
	}
	timestamp, err := strconv.ParseInt(dataParts[3], 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "can not parse timestamp of data point")
	}

	return &TSDBDataPoint{
		MetricsName: metricName,
		Labels:      tsdbUtils.LabelsFromStrings(labels...),
		Value:       val,
		Timestamp:   timestamp}, nil
}

func createTSDB() {
	adapter, err := tsdb.NewV3ioAdapter(v3ioConf, nil, nil)
	if err == nil {
		adapter.DeleteDB(true, true, 0, 0)
	}

	schm, err := schema.NewSchema(v3ioConf, "6/m", "1m", "max")
	if err != nil {
		log.Fatal("could not ctreate schema", err)
	}

	err = tsdb.CreateTSDB(v3ioConf, schm)
	if err != nil {
		log.Fatal("could not ctreate tsdb", err)
	}
}
