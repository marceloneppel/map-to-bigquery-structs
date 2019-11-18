package mtbgs

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
)

type MapToBigQueryStructs struct {
	TimestampFormat string
}

func Default() MapToBigQueryStructs {
	return MapToBigQueryStructs{
		TimestampFormat: time.RFC3339,
	}
}

func (mtbgs *MapToBigQueryStructs) Convert(inputMap map[string]interface{}, outputStruct interface{}) interface{} {
	structType := reflect.TypeOf(outputStruct)
	result := reflect.Indirect(reflect.New(structType))
	for field, value := range inputMap {
		// TODO: add conversion for bigquery.NullDate, bigquery.NullDateTime and bigquery.NullTime.
		switch result.FieldByName(field).Type().String() {
		case "bigquery.NullBool":
			switch reflect.ValueOf(value).Kind() {
			case reflect.Bool:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullBool{
					Bool:  value.(bool),
					Valid: true,
				}))
				break
			case reflect.String:
				parsedBool, err := strconv.ParseBool(value.(string))
				if err != nil {
					result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullBool{
						Valid: false,
					}))
					break
				}
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullBool{
					Bool:  parsedBool,
					Valid: true,
				}))
				break
			default:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullBool{
					Valid: false,
				}))
			}
			break
		case "bigquery.NullFloat64":
			switch reflect.ValueOf(value).Kind() {
			case reflect.Float64:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullFloat64{
					Float64: value.(float64),
					Valid:   true,
				}))
				break
			case reflect.Int:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullFloat64{
					Float64: float64(value.(int)),
					Valid:   true,
				}))
				break
			case reflect.Int64:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullFloat64{
					Float64: float64(value.(int64)),
					Valid:   true,
				}))
				break
			case reflect.String:
				parsedFloat64, err := strconv.ParseFloat(value.(string), 64)
				if err != nil {
					result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullFloat64{
						Valid: false,
					}))
					break
				}
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullFloat64{
					Float64: parsedFloat64,
					Valid:   true,
				}))
				break
			default:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullFloat64{
					Valid: false,
				}))
				break
			}
			break
		case "bigquery.NullInt64":
			switch reflect.ValueOf(value).Kind() {
			case reflect.Int:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullInt64{
					Int64: int64(value.(int)),
					Valid: true,
				}))
				break
			case reflect.Int64:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullInt64{
					Int64: value.(int64),
					Valid: true,
				}))
				break
			case reflect.String:
				parsedInt64, err := strconv.ParseInt(value.(string), 10, 64)
				if err != nil {
					result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullInt64{
						Valid: false,
					}))
					break
				}
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullInt64{
					Int64: parsedInt64,
					Valid: true,
				}))
				break
			default:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullInt64{
					Valid: false,
				}))
				break
			}
			break
		case "bigquery.NullString":
			switch reflect.ValueOf(value).Kind() {
			case reflect.Bool:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullString{
					StringVal: strconv.FormatBool(value.(bool)),
					Valid:     true,
				}))
				break
			case reflect.Float64:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullString{
					StringVal: strconv.FormatFloat(value.(float64), 'f', -1, 64),
					Valid:     true,
				}))
				break
			case reflect.String:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullString{
					StringVal: value.(string),
					Valid:     true,
				}))
				break
			default:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullString{
					Valid: false,
				}))
				break
			}
			break
		case "bigquery.NullTimestamp":
			switch reflect.ValueOf(value).Kind() {
			case reflect.String:
				parsedTimestamp, err := time.Parse(mtbgs.TimestampFormat, value.(string))
				if err != nil {
					fmt.Println(err)
					result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullTimestamp{
						Valid: false,
					}))
					break
				}
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullTimestamp{
					Timestamp: parsedTimestamp,
					Valid:     true,
				}))
				break
			case reflect.ValueOf(time.Time{}).Kind():
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullTimestamp{
					Timestamp: value.(time.Time),
					Valid:     true,
				}))
				break
			default:
				result.FieldByName(field).Set(reflect.ValueOf(bigquery.NullTimestamp{
					Valid: false,
				}))
				break
			}
			break
		default:
			fmt.Println(field, result.FieldByName(field).Type().String(), value)
			break
		}
	}
	return result.Interface()
}
