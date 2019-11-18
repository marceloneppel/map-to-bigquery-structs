package main

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	mtbqs "github.com/marceloneppel/map-to-bigquery-structs"
)

type MyExampleStruct struct {
	BoolField      bigquery.NullBool
	DateField      bigquery.NullDate
	DateTimeField  bigquery.NullDateTime
	Float64Field   bigquery.NullFloat64
	Int64Field     bigquery.NullInt64
	StringField    bigquery.NullString
	TimeField      bigquery.NullTime
	TimestampField bigquery.NullTimestamp
}

func main() {
	mtbqsInstance := mtbqs.Default()
	example0(mtbqsInstance)
	example1(mtbqsInstance)
	example2(mtbqsInstance)
	example3(mtbqsInstance)
}

func example0(instance mtbqs.MapToBigQueryStructs) {
	inputMap := map[string]interface{}{}
	result := instance.Convert(inputMap, MyExampleStruct{})
	fmt.Printf("result for example0: %+v\n", result)
}

func example1(instance mtbqs.MapToBigQueryStructs) {
	inputMap := map[string]interface{}{
		"BoolField":      true,
		"Float64Field":   1.5,
		"Int64Field":     1,
		"StringField":    "text",
		"TimestampField": time.Now(),
	}
	result := instance.Convert(inputMap, MyExampleStruct{})
	fmt.Printf("result for example1: %+v\n", result)
}

func example2(instance mtbqs.MapToBigQueryStructs) {
	inputMap := map[string]interface{}{
		"BoolField":      "true",
		"Float64Field":   "1.5",
		"Int64Field":     "1",
		"StringField":    true,
		"TimestampField": "1991-01-15T07:10:05-03:00",
	}
	result := instance.Convert(inputMap, MyExampleStruct{})
	fmt.Printf("result for example2: %+v\n", result)
}

func example3(instance mtbqs.MapToBigQueryStructs) {
	inputMap := map[string]interface{}{
		"Float64Field": 1,
		"StringField":  1.5,
	}
	result := instance.Convert(inputMap, MyExampleStruct{})
	fmt.Printf("result for example3: %+v\n", result)
}
