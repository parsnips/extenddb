-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0

-- Executed in the same transaction as the Rust backfill and finish.sql.
ALTER TABLE stream_records ADD COLUMN base_pk TEXT;
ALTER TABLE gsi_pending ADD COLUMN base_pk TEXT;
