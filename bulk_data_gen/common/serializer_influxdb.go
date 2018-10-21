package common

import (
	"io"
)

type serializerInflux struct {
}

func NewSerializerInflux() *serializerInflux {
	return &serializerInflux{}
}

// SerializeInfluxBulk writes Point data to the given writer, conforming to the
// InfluxDB wire protocol.
//
// This function writes output that looks like:
// <measurement>,<tag key>=<tag value> <field name>=<field value> <timestamp>\n
//
// For example:
// foo,tag0=bar baz=-1.0 100\n
//
// TODO(rw): Speed up this function. The bulk of time is spent in strconv.
func (s *serializerInflux) SerializePoint(w io.Writer, p *Point) (err error) {
	buf := scratchBufPool.Get().([]byte)
	buf = append(buf, p.MeasurementName...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, '=')
		buf = append(buf, p.TagValues[i]...)
	}

	if len(p.FieldKeys) > 0 {
		buf = append(buf, ' ')
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, '=')

		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, false)

		// Influx uses 'i' to indicate integers:
		switch v.(type) {
		case int, int64:
			buf = append(buf, 'i')
		}

		if i+1 < len(p.FieldKeys) {
			buf = append(buf, ',')
		}
	}

	buf = append(buf, ' ')
	buf = fastFormatAppend(p.Timestamp.UTC().UnixNano(), buf, true)
	buf = append(buf, '\n')
	_, err = w.Write(buf)

	buf = buf[:0]
	scratchBufPool.Put(buf)

	return err
}

func (s *serializerInflux) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}

func (s *serializerInflux) SerializeToCSV(w io.Writer, p *Point) error {
	return nil
}
