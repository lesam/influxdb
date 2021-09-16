package http

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/platform/backup"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"go.uber.org/zap"
)

// RestoreBackend is all services and associated parameters required to construct the RestoreHandler.
type RestoreBackend struct {
	Logger *zap.Logger
	errors.HTTPErrorHandler

	RestoreService          backup.RestoreService
	SqlBackupRestoreService backup.SqlBackupRestoreService
	BucketService           influxdb.BucketService
}

// NewRestoreBackend returns a new instance of RestoreBackend.
func NewRestoreBackend(b *APIBackend) *RestoreBackend {
	return &RestoreBackend{
		Logger: b.Logger.With(zap.String("handler", "restore")),

		HTTPErrorHandler:        b.HTTPErrorHandler,
		RestoreService:          b.RestoreService,
		SqlBackupRestoreService: b.SqlBackupRestoreService,
		BucketService:           b.BucketService,
	}
}

// RestoreHandler is http handler for restore service.
type RestoreHandler struct {
	*httprouter.Router
	api *kithttp.API
	errors.HTTPErrorHandler
	Logger *zap.Logger

	RestoreService          backup.RestoreService
	SqlBackupRestoreService backup.SqlBackupRestoreService
	BucketService           influxdb.BucketService
}

const (
	prefixRestore    = "/api/v2/restore"
	restoreKVPath    = prefixRestore + "/kv"
	restoreSqlPath   = prefixRestore + "/sql"
	restoreShardPath = prefixRestore + "/shards/:shardID"

	restoreBucketPath                   = prefixRestore + "/buckets/:bucketID" // Deprecated. Used by 2.0.x clients.
	restoreBucketMetadataDeprecatedPath = prefixRestore + "/bucket-metadata"   // Deprecated. Used by 2.1.0 of the CLI
	restoreBucketMetadataPath           = prefixRestore + "/bucketMetadata"
)

// NewRestoreHandler creates a new handler at /api/v2/restore to receive restore requests.
func NewRestoreHandler(b *RestoreBackend) *RestoreHandler {
	h := &RestoreHandler{
		HTTPErrorHandler:        b.HTTPErrorHandler,
		Router:                  NewRouter(b.HTTPErrorHandler),
		Logger:                  b.Logger,
		RestoreService:          b.RestoreService,
		SqlBackupRestoreService: b.SqlBackupRestoreService,
		BucketService:           b.BucketService,
		api:                     kithttp.NewAPI(kithttp.WithLog(b.Logger)),
	}

	h.HandlerFunc(http.MethodPost, restoreKVPath, h.handleRestoreKVStore)
	h.HandlerFunc(http.MethodPost, restoreSqlPath, h.handleRestoreSqlStore)
	h.HandlerFunc(http.MethodPost, restoreBucketPath, h.handleRestoreBucket)
	h.HandlerFunc(http.MethodPost, restoreBucketMetadataDeprecatedPath, h.handleRestoreBucketMetadata)
	h.HandlerFunc(http.MethodPost, restoreBucketMetadataPath, h.handleRestoreBucketMetadata)
	h.HandlerFunc(http.MethodPost, restoreShardPath, h.handleRestoreShard)

	return h
}

func (h *RestoreHandler) handleRestoreKVStore(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreKVStore")
	defer span.Finish()

	ctx := r.Context()

	var kvBytes io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzr, err := gzip.NewReader(kvBytes)
		if err != nil {
			err = &errors.Error{
				Code: errors.EInvalid,
				Msg:  "failed to decode gzip request body",
				Err:  err,
			}
			h.HandleHTTPError(ctx, err, w)
		}
		defer gzr.Close()
		kvBytes = gzr
	}

	if err := h.RestoreService.RestoreKVStore(ctx, kvBytes); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	res := backup.RestoreKVResponse{Token: "mytoken"}
	h.api.Respond(w, r, http.StatusOK, res)

}

func (h *RestoreHandler) handleRestoreSqlStore(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreSqlStore")
	defer span.Finish()

	ctx := r.Context()

	var sqlBytes io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzr, err := gzip.NewReader(sqlBytes)
		if err != nil {
			err = &errors.Error{
				Code: errors.EInvalid,
				Msg:  "failed to decode gzip request body",
				Err:  err,
			}
			h.HandleHTTPError(ctx, err, w)
		}
		defer gzr.Close()
		sqlBytes = gzr
	}

	if err := h.SqlBackupRestoreService.RestoreSqlStore(ctx, sqlBytes); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *RestoreHandler) handleRestoreBucket(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreBucket")
	defer span.Finish()

	ctx := r.Context()

	// Read bucket ID.
	bucketID, err := decodeIDFromCtx(r.Context(), "bucketID")
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	// Read serialized DBI data.
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	var dbi meta.DatabaseInfo
	if err := dbi.UnmarshalBinary(buf); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	shardIDMap, err := h.RestoreService.RestoreBucket(ctx, bucketID, dbi)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := json.NewEncoder(w).Encode(shardIDMap); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *RestoreHandler) handleRestoreBucketMetadata(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreBucketMetadata")
	defer span.Finish()
	ctx := r.Context()

	var b backup.BucketMetadataManifest
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Create the bucket - This will fail if the bucket already exists.
	// TODO: Could we support restoring to an existing bucket?
	var description string
	if b.Description != nil {
		description = *b.Description
	}
	var rp, sgd time.Duration
	if len(b.RetentionPolicies) > 0 {
		policy := b.RetentionPolicies[0]
		rp = policy.Duration
		sgd = policy.ShardGroupDuration
	}

	bkt := influxdb.Bucket{
		OrgID:              b.OrganizationID,
		Name:               b.BucketName,
		Description:        description,
		RetentionPeriod:    rp,
		ShardGroupDuration: sgd,
	}
	if err := h.BucketService.CreateBucket(ctx, &bkt); err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Restore shard-level metadata for the new bucket.
	dbi := manifestToDbInfo(b)
	shardIDMap, err := h.RestoreService.RestoreBucket(ctx, bkt.ID, dbi)
	if err != nil {
		h.Logger.Warn("Cleaning up after failed bucket-restore", zap.String("bucket_id", bkt.ID.String()))
		if err2 := h.BucketService.DeleteBucket(ctx, bkt.ID); err2 != nil {
			h.Logger.Error("Failed to clean up bucket after failed restore",
				zap.String("bucket_id", bkt.ID.String()), zap.Error(err2))
		}
		h.api.Err(w, r, err)
		return
	}

	res := backup.RestoredBucketMappings{
		ID:            bkt.ID,
		Name:          bkt.Name,
		ShardMappings: make([]backup.RestoredShardMapping, 0, len(shardIDMap)),
	}

	for old, new := range shardIDMap {
		res.ShardMappings = append(res.ShardMappings, backup.RestoredShardMapping{OldId: old, NewId: new})
	}

	h.api.Respond(w, r, http.StatusCreated, res)
}

func manifestToDbInfo(m backup.BucketMetadataManifest) meta.DatabaseInfo {
	dbi := meta.DatabaseInfo{
		Name:                   m.BucketName,
		DefaultRetentionPolicy: m.DefaultRetentionPolicy,
		RetentionPolicies:      make([]meta.RetentionPolicyInfo, len(m.RetentionPolicies)),
	}
	for i, rp := range m.RetentionPolicies {
		dbi.RetentionPolicies[i] = manifestToRpInfo(rp)
	}

	return dbi
}

func manifestToRpInfo(m backup.RetentionPolicyManifest) meta.RetentionPolicyInfo {
	rpi := meta.RetentionPolicyInfo{
		Name:               m.Name,
		ReplicaN:           m.ReplicaN,
		Duration:           m.Duration,
		ShardGroupDuration: m.ShardGroupDuration,
		ShardGroups:        make([]meta.ShardGroupInfo, len(m.ShardGroups)),
		Subscriptions:      make([]meta.SubscriptionInfo, len(m.Subscriptions)),
	}

	for i, sg := range m.ShardGroups {
		rpi.ShardGroups[i] = manifestToSgInfo(sg)
	}
	for i, s := range m.Subscriptions {
		rpi.Subscriptions[i] = meta.SubscriptionInfo{
			Name:         s.Name,
			Mode:         s.Mode,
			Destinations: s.Destinations,
		}
	}

	return rpi
}

func manifestToSgInfo(m backup.ShardGroupManifest) meta.ShardGroupInfo {
	var delAt, truncAt time.Time
	if m.DeletedAt != nil {
		delAt = *m.DeletedAt
	}
	if m.TruncatedAt != nil {
		truncAt = *m.TruncatedAt
	}
	sgi := meta.ShardGroupInfo{
		ID:          m.ID,
		StartTime:   m.StartTime,
		EndTime:     m.EndTime,
		DeletedAt:   delAt,
		TruncatedAt: truncAt,
		Shards:      make([]meta.ShardInfo, len(m.Shards)),
	}

	for i, sh := range m.Shards {
		sgi.Shards[i] = manifestToShardInfo(sh)
	}

	return sgi
}

func manifestToShardInfo(m backup.ShardManifest) meta.ShardInfo {
	si := meta.ShardInfo{
		ID:     m.ID,
		Owners: make([]meta.ShardOwner, len(m.ShardOwners)),
	}
	for i, so := range m.ShardOwners {
		si.Owners[i] = meta.ShardOwner{NodeID: so.NodeID}
	}

	return si
}

func (h *RestoreHandler) handleRestoreShard(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "RestoreHandler.handleRestoreShard")
	defer span.Finish()

	ctx := r.Context()

	params := httprouter.ParamsFromContext(ctx)
	shardID, err := strconv.ParseUint(params.ByName("shardID"), 10, 64)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	var tsmBytes io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzr, err := gzip.NewReader(tsmBytes)
		if err != nil {
			err = &errors.Error{
				Code: errors.EInvalid,
				Msg:  "failed to decode gzip request body",
				Err:  err,
			}
			h.HandleHTTPError(ctx, err, w)
		}
		defer gzr.Close()
		tsmBytes = gzr
	}

	if err := h.RestoreService.RestoreShard(ctx, shardID, tsmBytes); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}
