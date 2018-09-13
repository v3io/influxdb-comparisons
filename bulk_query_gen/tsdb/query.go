package tsdb

import (
	"fmt"
	"sync"
)

type TSDBQuery struct {
	HumanLabel       []byte
	HumanDescription []byte

	MetricName      []byte // e.g. "usage_user"
	AggregationType []byte // e.g. "avg" or "sum". used literally in the cassandra query.
	TimeStart       int64
	TimeEnd         int64
	Filter          []byte
	Step            int64
}

var TSDBQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &TSDBQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			MetricName:       []byte{},
			AggregationType:  []byte{},
		}
	},
}

func NewTSDBQuery() *TSDBQuery {
	return TSDBQueryPool.Get().(*TSDBQuery)
}

// String produces a debug-ready description of a Query.
func (q *TSDBQuery) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, MetricName: %s, AggregationType: %s, TimeStart: %s, TimeEnd: %s", q.HumanLabel, q.HumanDescription, q.MetricName, q.AggregationType, q.TimeStart, q.TimeEnd)
}

func (q *TSDBQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *TSDBQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *TSDBQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]

	q.MetricName = q.MetricName[:0]
	q.AggregationType = q.AggregationType[:0]
	q.TimeStart = 0
	q.TimeEnd = 0

	TSDBQueryPool.Put(q)
}
