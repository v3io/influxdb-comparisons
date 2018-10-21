package common

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
)

type Serializer interface {
	SerializePoint(w io.Writer, p *Point) error
	SerializeSize(w io.Writer, points int64, values int64) error
	SerializeToCSV(w io.Writer, p *Point) error
}

const DatasetSizeMarker = "dataset-size:"

var DatasetSizeMarkerRE = regexp.MustCompile(DatasetSizeMarker + `(\d+),(\d+)`)

func serializeSizeInText(w io.Writer, points int64, values int64) error {
	buf := scratchBufPool.Get().([]byte)
	buf = append(buf, fmt.Sprintf("%s%d,%d\n", DatasetSizeMarker, points, values)...)
	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func fastFormatAppend(v interface{}, buf []byte, singleQuotesForString bool) []byte {
	var quotationChar = "\""
	if singleQuotesForString {
		quotationChar = "'"
	}
	switch v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v.(int)), 10)
	case int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case float64:
		return strconv.AppendFloat(buf, v.(float64), 'f', 16, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', 16, 32)
	case bool:
		return strconv.AppendBool(buf, v.(bool))
	case []byte:
		buf = append(buf, quotationChar...)
		buf = append(buf, v.([]byte)...)
		buf = append(buf, quotationChar...)
		return buf
	case string:
		buf = append(buf, quotationChar...)
		buf = append(buf, v.(string)...)
		buf = append(buf, quotationChar...)
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
