package v1

import (
	"io"
	"os/exec"

	"github.com/sirupsen/logrus"

	"dioptra-io/ufuk-research/pkg/adapter"
)

// This basically converts the given warts file to a pantrace iris format. It is a jsonl
// format, slightly different than the results table format. Take a look at pantrace on
// Github.
type PantraceConverter struct {
	// The output of Convert functuon needs to be closed.
	adapter.ConvertCloser

	// name or the path of the pantrace executable.
	exec       string
	fromFormat string
	toFormat   string
	logger     *logrus.Logger
}

func NewPantraceConverter(logger *logrus.Logger) *PantraceConverter {
	// 	pantrace := exec.Command("pantrace", "--from", "scamper-trace-warts", "--to", "iris")
	return &PantraceConverter{
		exec:       "pantrace",
		fromFormat: "scamper-trace-warts",
		toFormat:   "iris",
		logger:     logger,
	}
}

// Note that the actual copying is done on a separate go routine. This means that if there
// an error on the conversion we won't be able to see the actual error. Instead we would
// only see the log, and the actual error will only be realized when trying to read from
// this function's output.
//
// To overcome this we might return an error chan instead of just an error. But I am too
// lazy to implement that rn.
func (p PantraceConverter) Convert(r io.Reader) (io.ReadCloser, error) {
	pantrace := exec.Command(
		p.exec,
		"--from",
		p.fromFormat,
		"--to",
		p.toFormat)

	stdin, err := pantrace.StdinPipe()
	if err != nil {
		return nil, err
	}

	stderr, err := pantrace.StderrPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := pantrace.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := pantrace.Start(); err != nil {
		return nil, err
	}

	// This is for piping the given reader as an argument to the stdin of the process.
	// A better method might be the usage of pipes, but for now this is works.
	go func() {
		defer stdin.Close()
		if numBytesRead, err := io.Copy(stdin, r); err != nil {
			p.logger.Panicf("Error while converting the wart using pantrace: %v.\n", err)
		} else {
			p.logger.Debugf("Conversion with pantrace resulted %v bytes.\n", numBytesRead)
		}
	}()

	// This is for displaying the error message pantrace gives out
	go func() {
		defer stderr.Close()
		if data, err := io.ReadAll(stderr); err != nil {
			p.logger.Panicf("Error while reading the std err of pantrace comamnd: %v.\n", err)
		} else if len(data) != 0 {
			p.logger.Panicf("Error while converting the pantrace: %v.\n", string(data))
		}
	}()

	return stdout, nil
}
