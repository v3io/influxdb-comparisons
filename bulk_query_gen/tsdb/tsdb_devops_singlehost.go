package tsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TSDBDevopsSingleHost produces TSDB-specific queries for the devops single-host case.
type TSDBDevopsSingleHost struct {
	TSDBDevops
}

func NewTSDBDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newTSDBDevopsCommon(dbConfig, start, end).(*TSDBDevops)
	return &TSDBDevopsSingleHost{
		TSDBDevops: *underlying,
	}
}

func (d *TSDBDevopsSingleHost) Dispatch(_, scaleVar int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
