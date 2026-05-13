package api

import (
	"errors"
	"fmt"
)

var (
	ErrNoTaskType          = errors.New("no task type set")
	ErrMultipleTaskType    = errors.New("multiple task types set")
	ErrNoGetTaskType       = errors.New("no get task type set")
	ErrMultipleGetTaskType = errors.New("multiple get task types set")
)

func (t *Task) Validate() error {
	if t == nil {
		return errors.New("task is nil")
	}

	count := 0

	if t.Get != nil {
		count++

		if err := t.Get.Validate(); err != nil {
			return fmt.Errorf("invalid get task: %w", err)
		}
	}

	switch count {
	case 0:
		return ErrNoTaskType
	case 1:
		return nil
	default:
		return ErrMultipleTaskType
	}
}

func (g *GetTask) Validate() error {
	if g == nil {
		return errors.New("get task is nil")
	}

	count := 0

	if g.Retina != nil {
		count++
	}

	if g.Iris != nil {
		count++
	}

	if g.Ark != nil {
		count++
	}

	switch count {
	case 0:
		return ErrNoGetTaskType
	case 1:
		return nil
	default:
		return ErrMultipleGetTaskType
	}
}
