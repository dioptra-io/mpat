package orm

import (
	"errors"
	"reflect"
	"strings"
)

func GetFieldJSONTags(row interface{}) ([]string, error) {
	var jsonNames []string

	v := reflect.ValueOf(row)
	t := reflect.TypeOf(row)

	for i := 0; i < v.NumField(); i++ {
		typeField := t.Field(i)

		jsonNames = append(jsonNames, typeField.Tag.Get("json"))
	}

	return jsonNames, nil
}

func GetInsertableFieldJSONTags(row interface{}) ([]string, error) {
	var jsonNames []string

	v := reflect.ValueOf(row)
	t := reflect.TypeOf(row)

	for i := 0; i < v.NumField(); i++ {
		typeField := t.Field(i)

		jsonTag := typeField.Tag.Get("json")
		mpatTag := typeField.Tag.Get("mpat")

		// of there is a tag `json:"my_field_name" mpat:"no_insert"`
		if !strings.Contains(mpatTag, "no_insert") {
			jsonNames = append(jsonNames, jsonTag)
		}

	}

	return jsonNames, nil
}

// HERE
func GetFieldPointers(row interface{}) ([]interface{}, error) {
	var fields []interface{}

	v := reflect.ValueOf(row)
	v = v.Elem()
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		valueField := v.Field(i)
		typeField := t.Field(i)

		if valueField.CanAddr() {
			fields = append(fields, valueField.Addr().Interface())
		} else {
			return nil, errors.New("cannot get address of field " + typeField.Name)
		}
	}

	return fields, nil
}

func GetInsertableFields(row interface{}) ([]interface{}, error) {
	var fields []interface{}

	v := reflect.ValueOf(row)
	v = v.Elem()
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		valueField := v.Field(i)
		typeField := t.Field(i)

		mpatTag := typeField.Tag.Get("mpat")

		if !strings.Contains(mpatTag, "no_insert") {
			fields = append(fields, valueField.Interface())
		}
	}

	return fields, nil
}
