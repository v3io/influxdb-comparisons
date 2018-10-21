package tsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TSDBDevopsSingleHost12hr produces TSDB-specific queries for the devops single-host case.
type TSDBDevopsSingleHost12hr struct {
	TSDBDevops
}

func NewTSDBDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTSDBDevopsCommon(dbConfig, interval, duration, scaleVar).(*TSDBDevops)
	return &TSDBDevopsSingleHost12hr{
		TSDBDevops: *underlying,
	}
}

func (d *TSDBDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
