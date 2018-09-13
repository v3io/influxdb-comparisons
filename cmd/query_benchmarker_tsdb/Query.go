package main

type TsdbQuery struct{
	HumanLabel       []byte
	HumanDescription []byte

	MetricName      []byte // e.g. "cpu_usage_user"
	AggregationType []byte // e.g. "avg" or "sum".
	TimeStart       int64
	TimeEnd         int64
	Filter          []byte
	Step            int64
}
