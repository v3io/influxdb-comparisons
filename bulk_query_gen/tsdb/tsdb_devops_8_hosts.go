package tsdb

import (
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"time"
)

// TSDBDevops8Hosts produces TSDB-specific queries for the devops groupby case.
type TSDBDevops8Hosts struct {
	TSDBDevops
}

func NewTSDBDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTSDBDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TSDBDevops)
	return &TSDBDevops8Hosts{
		TSDBDevops: *underlying,
	}
}

func (d *TSDBDevops8Hosts) Dispatch(i int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}
