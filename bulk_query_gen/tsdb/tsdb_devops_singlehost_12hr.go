package tsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TSDBDevopsSingleHost12hr produces TSDB-specific queries for the devops single-host case.
type TSDBDevopsSingleHost12hr struct {
	TSDBDevops
}

func NewTSDBDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newTSDBDevopsCommon(dbConfig, start, end).(*TSDBDevops)
	return &TSDBDevopsSingleHost12hr{
		TSDBDevops: *underlying,
	}
}

func (d *TSDBDevopsSingleHost12hr) Dispatch(_, scaleVar int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
