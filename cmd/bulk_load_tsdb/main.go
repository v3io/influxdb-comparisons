package main

import (
	"bufio"
	"flag"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	tsdbConfig "github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	tsdbUtils "github.com/v3io/v3io-tsdb/pkg/utils"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"github.com/v3io/v3io-tsdb/pkg/performance"
)

// Program option vars:
var (
	itemLimit      int64
	workers        int
	serverPath     string
	dbPath         string
	configFilePath string
)

// Global vars
var (
	inputDone    chan struct{}
	workersGroup sync.WaitGroup

	tsdbAdapter   *tsdb.V3ioAdapter
	tsdbAppender  tsdb.Appender
	batchTSDBChan chan *TSDBDataPoint
	v3ioConf      *tsdbConfig.V3ioConfig
)

type TSDBDataPoint struct {
	MetricsName string
	Labels      tsdbUtils.Labels
	Value       float64
	Timestamp   int64
}

func init() {
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.Int64Var(&itemLimit, "itemsLimit", -1, "Number of parallel requests to make.")
	flag.StringVar(&serverPath, "server", "", "V3IO Service URL - username:password@ip:port/container")
	flag.StringVar(&dbPath, "dbPath", "", "sub path for the TSDB, inside the container")
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
		v3ioConf.V3ioUrl = serverPath[0:slash]
		v3ioConf.Container = serverPath[slash+1:]
	}
	if dbPath != "" {
		v3ioConf.Path = dbPath
	}


	log.Printf("conf is: %v", v3ioConf)
	batchTSDBChan = make(chan *TSDBDataPoint, workers)
}

func main() {
	// Measure performance
	metricReporter, _ := performance.DefaultReporterInstance()
	metricReporter.Start()
	defer metricReporter.Stop()

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
		go processRecords()
	}

	start := time.Now()
	itemsRead, bytesRead := scan()
	close(batchTSDBChan)
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
		batchTSDBChan <- data

		itemsRead++
		bytesRead += int64(len(scanner.Bytes()))
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	//Closing inputDone signals to the application that we've read everything and can now shut down.
	//close(inputDone)

	return itemsRead, bytesRead
}

func processRecords() {
	var total int64
	for data := range batchTSDBChan {
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

	tsdbAppender.WaitForCompletion(0)
	log.Println("Process time: ", total)
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
		Labels:      tsdbUtils.FromStrings(labels...),
		Value:       val,
		Timestamp:   timestamp}, nil
}

func createTSDB() {
	adapter, err := tsdb.NewV3ioAdapter(v3ioConf, nil, nil)
	if err == nil {
		adapter.DeleteDB(true, true, 0, 0)
	}

	rollups, err := aggregate.AggregatorsToStringList("")
	if err != nil {
		log.Fatal(err)
	}
	defaultRollup := tsdbConfig.Rollup{
		Aggregators:            rollups,
		AggregatorsGranularity: "1m",
		StorageClass:           "local",
		SampleRetention:        0,
		LayerRetentionTime:     "1y",
	}

	tableSchema := tsdbConfig.TableSchema{
		Version:             0,
		RollupLayers:        []tsdbConfig.Rollup{defaultRollup},
		ShardingBuckets:     8,
		PartitionerInterval: "290h",
		ChunckerInterval:    "10h",
	}

	fields, err := aggregate.SchemaFieldFromString(rollups, "v")
	if err != nil {
		log.Fatal("Failed to create aggregators list", err)
	}
	fields = append(fields, tsdbConfig.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})

	partitionSchema := tsdbConfig.PartitionSchema{
		Version:                tableSchema.Version,
		Aggregators:            rollups,
		AggregatorsGranularity: "1m",
		StorageClass:           "local",
		SampleRetention:        0,
		ChunckerInterval:       tableSchema.ChunckerInterval,
		PartitionerInterval:    tableSchema.PartitionerInterval,
	}

	schema := &tsdbConfig.Schema{
		TableSchemaInfo:     tableSchema,
		PartitionSchemaInfo: partitionSchema,
		Fields:              fields,
	}

	err = tsdb.CreateTSDB(v3ioConf, schema)
	if err != nil {
		log.Fatal("could not ctreate tsdb", err)
	}
}
