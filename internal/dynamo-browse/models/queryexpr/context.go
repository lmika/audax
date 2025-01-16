package queryexpr

import (
	"context"
	"time"

	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models"
)

type timeSource interface {
	now() time.Time
}

type defaultTimeSource struct{}

func (tds defaultTimeSource) now() time.Time {
	return time.Now()
}

type timeSourceContextKeyType struct{}

var timeSourceContextKey = timeSourceContextKeyType{}

func timeSourceFromContext(ctx context.Context) timeSource {
	if tts, ok := ctx.Value(timeSourceContextKey).(timeSource); ok {
		return tts
	}
	return defaultTimeSource{}
}

type currentResultSetContextKeyType struct{}

var currentResultSetContextKey = currentResultSetContextKeyType{}

func currentResultSetFromContext(ctx context.Context) *models.ResultSet {
	if crs, ok := ctx.Value(currentResultSetContextKey).(*models.ResultSet); ok {
		return crs
	}
	return nil
}

type currentPasteboardControllerKeyType struct{}

var currentPasteboardControllerKey = currentPasteboardControllerKeyType{}

func currentPasteboardControllerFromContext(ctx context.Context) PasteBoardController {
	if crs, ok := ctx.Value(currentPasteboardControllerKey).(PasteBoardController); ok {
		return crs
	}
	return nil
}

type PasteBoardController interface {
	ReadText() (string, bool)
}
