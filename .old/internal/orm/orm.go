package orm

import (
	"errors"
	"reflect"
)

var ErrArgumentIsNotAStructPointer = errors.New("given argument is not a struct pointer")

type ormFilter struct {
	obj     any
	include []string
	exclude []string
}

func NewFrom(obj any, include []string, exclude []string) (*ormFilter, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()

	if typ.Kind() != reflect.Ptr {
		return nil, ErrArgumentIsNotAStructPointer
	}
	if val.Elem().Kind() != reflect.Struct {
		return nil, ErrArgumentIsNotAStructPointer
	}

	return &ormFilter{
		obj:     obj,
		include: include,
		exclude: exclude,
	}, nil
}

func New(obj any) (*ormFilter, error) {
	return &ormFilter{
		obj:     obj,
		include: []string{},
		exclude: []string{},
	}, nil
}

// func (o *ormFilter) IncludeTag()
