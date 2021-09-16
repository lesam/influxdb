package backup

import (
	"context"
	"encoding/json"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/platform/backup"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
)

type BucketManifestWriter struct {
	ts *tenant.Service
	mc *meta.Client
}

func NewBucketManifestWriter(ts *tenant.Service, mc *meta.Client) BucketManifestWriter {
	return BucketManifestWriter{
		ts: ts,
		mc: mc,
	}
}

// WriteManifest writes a bucket manifest describing all of the buckets that exist in the database.
// It is intended to be used to write to an HTTP response after appropriate measures have been taken
// to ensure that the request is authorized.
func (b BucketManifestWriter) WriteManifest(ctx context.Context, w io.Writer) error {
	bkts, _, err := b.ts.FindBuckets(ctx, influxdb.BucketFilter{})
	if err != nil {
		return err
	}

	l := make([]backup.BucketMetadataManifest, 0, len(bkts))

	for _, bkt := range bkts {
		org, err := b.ts.OrganizationService.FindOrganizationByID(ctx, bkt.OrgID)
		if err != nil {
			return err
		}

		dbInfo := b.mc.Database(bkt.ID.String())

		var description *string
		if bkt.Description != "" {
			description = &bkt.Description
		}

		l = append(l, backup.BucketMetadataManifest{
			OrganizationID:         bkt.OrgID,
			OrganizationName:       org.Name,
			BucketID:               bkt.ID,
			BucketName:             bkt.Name,
			Description:            description,
			DefaultRetentionPolicy: dbInfo.DefaultRetentionPolicy,
			RetentionPolicies:      retentionPolicyToManifest(dbInfo.RetentionPolicies),
		})
	}

	return json.NewEncoder(w).Encode(&l)
}

// retentionPolicyToManifest and the various similar functions that follow are for converting
// from the structs in the meta package to the manifest structs
func retentionPolicyToManifest(meta []meta.RetentionPolicyInfo) []backup.RetentionPolicyManifest {
	r := make([]backup.RetentionPolicyManifest, 0, len(meta))

	for _, m := range meta {
		r = append(r, backup.RetentionPolicyManifest{
			Name:               m.Name,
			ReplicaN:           m.ReplicaN,
			Duration:           m.Duration,
			ShardGroupDuration: m.ShardGroupDuration,
			ShardGroups:        shardGroupToManifest(m.ShardGroups),
			Subscriptions:      subscriptionInfosToManifest(m.Subscriptions),
		})
	}

	return r
}

func subscriptionInfosToManifest(subInfos []meta.SubscriptionInfo) []backup.SubscriptionManifest {
	r := make([]backup.SubscriptionManifest, 0, len(subInfos))

	for _, s := range subInfos {
		r = append(r, backup.SubscriptionManifest(s))
	}

	return r
}

func shardGroupToManifest(shardGroups []meta.ShardGroupInfo) []backup.ShardGroupManifest {
	r := make([]backup.ShardGroupManifest, 0, len(shardGroups))

	for _, s := range shardGroups {
		deletedAt := &s.DeletedAt
		truncatedAt := &s.TruncatedAt

		// set deletedAt and truncatedAt to nil rather than their zero values so that the fields
		// can be properly omitted from the JSON response if they are empty
		if deletedAt.IsZero() {
			deletedAt = nil
		}

		if truncatedAt.IsZero() {
			truncatedAt = nil
		}

		r = append(r, backup.ShardGroupManifest{
			ID:          s.ID,
			StartTime:   s.StartTime,
			EndTime:     s.EndTime,
			DeletedAt:   deletedAt,
			TruncatedAt: truncatedAt,
			Shards:      shardInfosToManifest(s.Shards),
		})
	}

	return r
}

func shardInfosToManifest(shards []meta.ShardInfo) []backup.ShardManifest {
	r := make([]backup.ShardManifest, 0, len(shards))

	for _, s := range shards {
		r = append(r, backup.ShardManifest{
			ID:          s.ID,
			ShardOwners: shardOwnersToManifest(s.Owners),
		})
	}

	return r
}

func shardOwnersToManifest(shardOwners []meta.ShardOwner) []backup.ShardOwner {
	r := make([]backup.ShardOwner, 0, len(shardOwners))

	for _, s := range shardOwners {
		r = append(r, backup.ShardOwner(s))
	}

	return r
}
