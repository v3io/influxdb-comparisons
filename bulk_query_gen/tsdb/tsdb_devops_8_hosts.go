package tsdb

import (
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"time"
)

// TSDBDevops8Hosts produces TSDB-specific queries for the devops groupby case.
type TSDBDevops8Hosts struct {
	TSDBDevops
}

func NewTSDBDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newTSDBDevopsCommon(dbConfig, start, end).(*TSDBDevops)
	return &TSDBDevops8Hosts{
		TSDBDevops: *underlying,
	}
}

func (d *TSDBDevops8Hosts) Dispatch(_, scaleVar int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
