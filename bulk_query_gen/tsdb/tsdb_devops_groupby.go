package tsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TSDBDevopsGroupby produces TSDB-specific queries for the devops groupby case.
type TSDBDevopsGroupby struct {
	TSDBDevops
}

func NewTSDBDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTSDBDevopsCommon(dbConfig, interval, duration, scaleVar).(*TSDBDevops)
	return &TSDBDevopsGroupby{
		TSDBDevops: *underlying,
	}

}

func (d *TSDBDevopsGroupby) Dispatch(i int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q)
	return q
}
