package runutil

import (
	"io"

	"github.com/pkg/errors"

	"github.com/grafana/dskit/multierror"
)

// CloseWithErrCapture closes closer and wraps any error with the provided message and assigns it to err.
func CloseWithErrCapture(err *error, closer io.Closer, msg string) {
	merr := multierror.New(*err, errors.Wrap(closer.Close(), msg))
	*err = merr.Err()
}
