// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/user/logging.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package user

import (
	"context"

	"github.com/go-kit/log"
)

// LogWith returns user and org information from the context as log fields.
func LogWith(ctx context.Context, logger log.Logger) log.Logger {
	userID, err := ExtractUserID(ctx)
	logergWithFields := logger
	if err == nil {
		logergWithFields = log.With(logergWithFields, "userID", userID)
	}

	orgID, err := ExtractOrgID(ctx)
	if err == nil {
		logergWithFields = log.With(logergWithFields, "orgID", orgID)
	}

	return logergWithFields
}
