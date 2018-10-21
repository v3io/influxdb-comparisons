package tsdb

import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"strings"
	"time"
)

type TSDBDevops struct {
	bulkQuerygen.CommonParams
	dbPath      string
	AllInterval bulkQuerygen.TimeInterval
}

func newTSDBDevopsCommon(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return &TSDBDevops{
		dbPath:      dbConfig["database-name"],
		AllInterval: interval,
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *TSDBDevops) Dispatch(i int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	bulkQuerygen.DevopsDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), 1, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), 2, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteFourHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), 4, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteEightHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), 8, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteSixteenHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), 16, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), 32, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsage12HoursByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), 1, 12*time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *TSDBDevops) maxCPUUsageHourByMinuteNHosts(qi bulkQuerygen.Query, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nhosts]

	hostFilters := []string{}
	for _, n := range nn {
		hostname := fmt.Sprintf("host_%d", n)
		tag := fmt.Sprintf("hostname=='%s'", hostname)
		hostFilters = append(hostFilters, tag)
	}

	filter := strings.Join(hostFilters, " and ")
	humanLabel := fmt.Sprintf("tsdb max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*TSDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("max")
	q.MetricName = []byte("cpu_usage_user")

	// get time in millis
	q.TimeStart = interval.Start.UnixNano() / int64(time.Millisecond)
	q.TimeEnd = interval.End.UnixNano() / int64(time.Millisecond)
	q.Step = int64(time.Minute / time.Millisecond) // 1m interval
	q.Filter = []byte(filter)
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *TSDBDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi bulkQuerygen.Query) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	humanLabel := "tsdb mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*TSDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("avg")
	q.MetricName = []byte("cpu_usage_user")

	// get time in millis
	q.TimeStart = interval.Start.UnixNano() / int64(time.Millisecond)
	q.TimeEnd = interval.End.UnixNano() / int64(time.Millisecond)
	q.Step = int64(time.Hour / time.Millisecond)
}
