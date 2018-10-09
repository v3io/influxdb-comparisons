package common

import (
	"fmt"
	"io"
	"time"
)

type SerializerTSDB struct {
}

func NewSerializerTSDB() *SerializerTSDB {
	return &SerializerTSDB{}
}

// Outputs TSDB data.
// We refer to a field as a metric, hence the output is one row per field where the 'MeasurementName' is just another label
//
// <measurement>_<field name>#labelKey=labelValue...#fieldValue#timestamp
// For Example:
// cpu_usage_user#measurement=cpu,hostname=host_0,region=us-west-1,datacenter=us-west-1b,rack=1,os=Ubuntu16.10#84.123#1514764800000
func (s *SerializerTSDB) SerializePoint(w io.Writer, p *Point) error {
	labels := make([]byte, 0, 256)
	buf := scratchBufPool.Get().([]byte)
	metricType := fmt.Sprintf("measurement %s", p.MeasurementName)
	labels = append(labels, []byte(metricType)...)

	for i := 0; i < len(p.TagKeys); i++ {
		labels = append(labels, byte(' '))
		labels = append(labels, p.TagKeys[i]...)
		labels = append(labels, byte(' '))
		labels = append(labels, p.TagValues[i]...)
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, []byte(fmt.Sprintf("%s_", p.MeasurementName))...)
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, byte('#'))
		buf = append(buf, labels...)
		buf = append(buf, byte('#'))
		buf = fastFormatAppend(p.FieldValues[i], buf, true)
		buf = append(buf, byte('#'))
		buf = fastFormatAppend(p.Timestamp.UnixNano()/int64(time.Millisecond), buf, true)
		buf = append(buf, '\n')
	}

	_, err := w.Write(buf)

	buf = buf[:0]
	scratchBufPool.Put(buf)

	return err
}

func (s *SerializerTSDB) SerializeSize(w io.Writer, points int64, values int64) error {
	//return serializeSizeInText(w, points, values)
	return nil
}

func (s *SerializerTSDB) SerializeToCSV(w io.Writer, p *Point) error{

	labels := make([]byte, 0, 256)
	buf := scratchBufPool.Get().([]byte)
	metricType := fmt.Sprintf("measurement=%s", p.MeasurementName)
	labels = append(labels, byte('"'))
	labels = append(labels, []byte(metricType)...)

	for i := 0; i < len(p.TagKeys); i++ {
		labels = append(labels, byte(','))
		labels = append(labels, p.TagKeys[i]...)
		labels = append(labels, byte('='))
		labels = append(labels, p.TagValues[i]...)
	}
	labels = append(labels, byte('"'))

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, []byte(fmt.Sprintf("%s_", p.MeasurementName))...)
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, byte(','))
		buf = append(buf, labels...)
		buf = append(buf, byte(','))
		buf = fastFormatAppend(p.FieldValues[i], buf, false)
		buf = append(buf, byte(','))
		buf = fastFormatAppend(p.Timestamp.UnixNano()/int64(time.Millisecond), buf, true)
		buf = append(buf, '\n')
	}

	_, err := w.Write(buf)

	buf = buf[:0]
	scratchBufPool.Put(buf)

	return err
}