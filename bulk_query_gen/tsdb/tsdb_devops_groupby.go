package tsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TSDBDevopsGroupby produces TSDB-specific queries for the devops groupby case.
type TSDBDevopsGroupby struct {
	TSDBDevops
}

func NewTSDBDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newTSDBDevopsCommon(dbConfig, start, end).(*TSDBDevops)
	return &TSDBDevopsGroupby{
		TSDBDevops: *underlying,
	}

}

func (d *TSDBDevopsGroupby) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
