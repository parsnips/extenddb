-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE stream_records ALTER COLUMN base_pk SET NOT NULL;
ALTER TABLE gsi_pending ALTER COLUMN base_pk SET NOT NULL;

COMMENT ON COLUMN stream_records.base_pk IS
    'Exact encoded base-table pk; physical routing key, independent of logical shard_id';
COMMENT ON COLUMN gsi_pending.base_pk IS
    'Exact encoded base-table pk; physical routing key, independent of worker_partition';

-- Bound the enqueue-time ordering lookup to one base partition key.
CREATE INDEX idx_gsi_pending_base_key_ready ON gsi_pending (table_id, base_pk, ready_at);
