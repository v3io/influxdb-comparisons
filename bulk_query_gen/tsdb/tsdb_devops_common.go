package tsdb

import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"strings"
	"time"
)

type TSDBDevops struct {
	dbPath      string
	AllInterval bulkQuerygen.TimeInterval
}

func newTSDBDevopsCommon(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}

	return &TSDBDevops{
		dbPath:      dbConfig["database-name"],
		AllInterval: bulkQuerygen.NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *TSDBDevops) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewTSDBQuery() // from pool
	bulkQuerygen.DevopsDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteOneHost(q bulkQuerygen.Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), scaleVar, 1, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteTwoHosts(q bulkQuerygen.Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), scaleVar, 2, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteFourHosts(q bulkQuerygen.Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), scaleVar, 4, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteEightHosts(q bulkQuerygen.Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), scaleVar, 8, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteSixteenHosts(q bulkQuerygen.Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), scaleVar, 16, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q bulkQuerygen.Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), scaleVar, 32, time.Hour)
}

func (d *TSDBDevops) MaxCPUUsage12HoursByMinuteOneHost(q bulkQuerygen.Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*TSDBQuery), scaleVar, 1, 12*time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *TSDBDevops) maxCPUUsageHourByMinuteNHosts(qi bulkQuerygen.Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(scaleVar)[:nhosts]

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
func (d *TSDBDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi bulkQuerygen.Query, _ int) {
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
