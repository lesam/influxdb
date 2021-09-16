package authorizer

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/platform/backup"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
)

var _ backup.RestoreService = (*RestoreService)(nil)

// RestoreService wraps a influxdb.RestoreService and authorizes actions
// against it appropriately.
type RestoreService struct {
	s backup.RestoreService
}

// NewRestoreService constructs an instance of an authorizing restore service.
func NewRestoreService(s backup.RestoreService) *RestoreService {
	return &RestoreService{
		s: s,
	}
}

func (b RestoreService) RestoreKVStore(ctx context.Context, r io.Reader) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := IsAllowedAll(ctx, influxdb.OperPermissions()); err != nil {
		return err
	}
	return b.s.RestoreKVStore(ctx, r)
}

func (b RestoreService) RestoreBucket(ctx context.Context, id platform.ID, dbi meta.DatabaseInfo) (shardIDMap map[uint64]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := IsAllowedAll(ctx, influxdb.OperPermissions()); err != nil {
		return nil, err
	}
	return b.s.RestoreBucket(ctx, id, dbi)
}

func (b RestoreService) RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := IsAllowedAll(ctx, influxdb.OperPermissions()); err != nil {
		return err
	}
	return b.s.RestoreShard(ctx, shardID, r)
}
