package tsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TSDBDevopsSingleHost produces TSDB-specific queries for the devops single-host case.
type TSDBDevopsSingleHost struct {
	TSDBDevops
}

func NewTSDBDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTSDBDevopsCommon(dbConfig, interval, duration, scaleVar).(*TSDBDevops)
	return &TSDBDevopsSingleHost{
		TSDBDevops: *underlying,
	}
}

func (d *TSDBDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}
