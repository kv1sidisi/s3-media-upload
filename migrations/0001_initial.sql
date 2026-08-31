CREATE TABLE schema_migrations (
    version bigint PRIMARY KEY,
    applied_at timestamptz NOT NULL
);

CREATE TABLE uploads (
    upload_id uuid PRIMARY KEY,
    idempotency_key uuid NOT NULL,
    staging_key text NOT NULL,
    declared_size bigint NOT NULL,
    declared_content_type text NOT NULL,
    state text NOT NULL,
    created_at timestamptz NOT NULL,
    upload_deadline timestamptz NOT NULL,
    max_write_expires_at timestamptz NOT NULL,
    completion_requested_at timestamptz,
    reconcile_after timestamptz,
    reconcile_failure_streak integer NOT NULL DEFAULT 0,
    reconcile_last_failure_class text,
    reconcile_last_failure_at timestamptz,
    claim_token uuid,
    claim_expires_at timestamptz,
    terminal_at timestamptz,
    final_key text,
    rejection_class text,
    rejection_reason text,
    rejection_policy_version smallint,
    rejection_phase text,
    rejection_evidence jsonb,
    expiry_reason text,

    CONSTRAINT uploads_idempotency_key_key
        UNIQUE (idempotency_key) NOT DEFERRABLE,
    CONSTRAINT uploads_staging_key_check
        CHECK (staging_key = 'staging/' || upload_id::text),
    CONSTRAINT uploads_declared_size_check
        CHECK (declared_size BETWEEN 1 AND 10485760),
    CONSTRAINT uploads_declared_content_type_check
        CHECK (declared_content_type IN ('image/jpeg', 'image/png')),
    CONSTRAINT uploads_state_check
        CHECK (state IN ('pending', 'finalizing', 'ready', 'rejected', 'expired')),
    CONSTRAINT uploads_upload_deadline_check
        CHECK (upload_deadline = created_at + interval '24 hours'),
    CONSTRAINT uploads_write_horizon_check
        CHECK (
            max_write_expires_at > created_at
            AND max_write_expires_at <= upload_deadline + interval '15 minutes'
        ),
    CONSTRAINT uploads_completion_deadline_check
        CHECK (
            completion_requested_at IS NULL
            OR completion_requested_at < upload_deadline
        ),
    CONSTRAINT uploads_reconcile_failure_check
        CHECK (
            (
                reconcile_failure_streak = 0
                AND reconcile_last_failure_class IS NULL
                AND reconcile_last_failure_at IS NULL
            )
            OR
            (
                reconcile_failure_streak > 0
                AND reconcile_last_failure_class IS NOT NULL
                AND reconcile_last_failure_class IN (
                    'transient',
                    'ambiguous',
                    'auth',
                    'configuration',
                    'other_deterministic'
                )
                AND reconcile_last_failure_at IS NOT NULL
            )
        ),
    CONSTRAINT uploads_claim_pair_check
        CHECK ((claim_token IS NULL) = (claim_expires_at IS NULL)),
    CONSTRAINT uploads_claim_state_check
        CHECK (claim_token IS NULL OR state = 'finalizing'),
    CONSTRAINT uploads_final_key_state_check
        CHECK ((final_key IS NOT NULL) = (state = 'ready')),
    CONSTRAINT uploads_rejection_phase_check
        CHECK (
            rejection_phase IS NULL
            OR rejection_phase IN (
                'staging_read',
                'decode_config',
                'decode',
                'candidate_verify'
            )
        ),
    CONSTRAINT uploads_rejection_reason_check
        CHECK (
            rejection_class IS NULL
            OR (
                rejection_class = 'invalid_input'
                AND rejection_reason IS NOT NULL
                AND rejection_reason IN (
                    'object_too_large',
                    'dimensions_limit_exceeded',
                    'pixel_limit_exceeded',
                    'declared_size_mismatch',
                    'invalid_image_encoding',
                    'declared_content_type_mismatch',
                    'malformed_image'
                )
            )
            OR (
                rejection_class = 'internal_invariant'
                AND rejection_reason IS NOT NULL
                AND rejection_reason IN (
                    'decoder_invariant_mismatch',
                    'candidate_integrity_mismatch'
                )
            )
        ),
    CONSTRAINT uploads_state_shape_check
        CHECK (
            CASE state
                WHEN 'pending' THEN
                    completion_requested_at IS NULL
                    AND reconcile_after IS NULL
                    AND reconcile_failure_streak = 0
                    AND claim_token IS NULL
                    AND terminal_at IS NULL
                    AND final_key IS NULL
                    AND rejection_class IS NULL
                    AND rejection_reason IS NULL
                    AND rejection_policy_version IS NULL
                    AND rejection_phase IS NULL
                    AND rejection_evidence IS NULL
                    AND expiry_reason IS NULL
                WHEN 'finalizing' THEN
                    completion_requested_at IS NOT NULL
                    AND reconcile_after IS NOT NULL
                    AND terminal_at IS NULL
                    AND final_key IS NULL
                    AND rejection_class IS NULL
                    AND rejection_reason IS NULL
                    AND rejection_policy_version IS NULL
                    AND rejection_phase IS NULL
                    AND rejection_evidence IS NULL
                    AND expiry_reason IS NULL
                WHEN 'ready' THEN
                    completion_requested_at IS NOT NULL
                    AND reconcile_after IS NULL
                    AND claim_token IS NULL
                    AND terminal_at IS NOT NULL
                    AND final_key IS NOT NULL
                    AND rejection_class IS NULL
                    AND rejection_reason IS NULL
                    AND rejection_policy_version IS NULL
                    AND rejection_phase IS NULL
                    AND rejection_evidence IS NULL
                    AND expiry_reason IS NULL
                WHEN 'rejected' THEN
                    completion_requested_at IS NOT NULL
                    AND reconcile_after IS NULL
                    AND claim_token IS NULL
                    AND terminal_at IS NOT NULL
                    AND final_key IS NULL
                    AND rejection_class IS NOT NULL
                    AND rejection_reason IS NOT NULL
                    AND rejection_policy_version IS NOT NULL
                    AND rejection_policy_version = 1
                    AND rejection_phase IS NOT NULL
                    AND rejection_evidence IS NOT NULL
                    AND jsonb_typeof(rejection_evidence) = 'object'
                    AND rejection_evidence <> '{}'::jsonb
                    AND expiry_reason IS NULL
                WHEN 'expired' THEN
                    reconcile_after IS NULL
                    AND claim_token IS NULL
                    AND terminal_at IS NOT NULL
                    AND final_key IS NULL
                    AND rejection_class IS NULL
                    AND rejection_reason IS NULL
                    AND rejection_policy_version IS NULL
                    AND rejection_phase IS NULL
                    AND rejection_evidence IS NULL
                    AND expiry_reason IS NOT NULL
                    AND (
                        (
                            expiry_reason = 'upload_deadline_elapsed'
                            AND completion_requested_at IS NULL
                        )
                        OR
                        (
                            expiry_reason = 'staging_missing_after_write_window'
                            AND completion_requested_at IS NOT NULL
                            AND terminal_at >= GREATEST(
                                upload_deadline,
                                max_write_expires_at
                            )
                        )
                    )
                ELSE false
            END
        )
);

CREATE TABLE upload_candidates (
    upload_id uuid NOT NULL,
    sha256 bytea NOT NULL,
    object_key text NOT NULL,
    encoded_size bigint NOT NULL,
    validation_policy_version smallint NOT NULL,
    image_format text NOT NULL,
    width integer NOT NULL,
    height integer NOT NULL,
    registered_at timestamptz NOT NULL,

    CONSTRAINT upload_candidates_pkey
        PRIMARY KEY (upload_id, sha256),
    CONSTRAINT upload_candidates_upload_id_object_key_key
        UNIQUE (upload_id, object_key),
    CONSTRAINT upload_candidates_upload_id_fkey
        FOREIGN KEY (upload_id)
        REFERENCES uploads (upload_id)
        ON DELETE RESTRICT,
    CONSTRAINT upload_candidates_sha256_check
        CHECK (octet_length(sha256) = 32),
    CONSTRAINT upload_candidates_object_key_check
        CHECK (
            object_key = 'media/' || upload_id::text || '/' || encode(sha256, 'hex')
        ),
    CONSTRAINT upload_candidates_encoded_size_check
        CHECK (encoded_size BETWEEN 1 AND 10485760),
    CONSTRAINT upload_candidates_policy_check
        CHECK (validation_policy_version = 1),
    CONSTRAINT upload_candidates_image_format_check
        CHECK (image_format IN ('jpeg', 'png')),
    CONSTRAINT upload_candidates_dimensions_check
        CHECK (width BETWEEN 1 AND 8192 AND height BETWEEN 1 AND 8192),
    CONSTRAINT upload_candidates_pixels_check
        CHECK (width::bigint * height::bigint <= 8388608)
);

ALTER TABLE uploads
    ADD CONSTRAINT uploads_final_candidate_fkey
    FOREIGN KEY (upload_id, final_key)
    REFERENCES upload_candidates (upload_id, object_key)
    ON DELETE RESTRICT;

CREATE TABLE cleanup_tombstones (
    object_key text PRIMARY KEY,
    upload_id uuid NOT NULL,
    candidate_sha256 bytea,
    created_at timestamptz NOT NULL,
    delete_not_before timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL,
    reservation_token uuid,
    reservation_expires_at timestamptz,
    failure_streak integer NOT NULL DEFAULT 0,
    last_attempt_at timestamptz,
    last_result text,

    CONSTRAINT cleanup_tombstones_upload_id_fkey
        FOREIGN KEY (upload_id)
        REFERENCES uploads (upload_id)
        ON DELETE RESTRICT,
    CONSTRAINT cleanup_tombstones_candidate_fkey
        FOREIGN KEY (upload_id, candidate_sha256)
        REFERENCES upload_candidates (upload_id, sha256)
        ON DELETE RESTRICT,
    CONSTRAINT cleanup_tombstones_object_shape_check
        CHECK (
            (
                candidate_sha256 IS NULL
                AND object_key = 'staging/' || upload_id::text
            )
            OR
            (
                candidate_sha256 IS NOT NULL
                AND octet_length(candidate_sha256) = 32
                AND object_key = 'media/' || upload_id::text || '/'
                    || encode(candidate_sha256, 'hex')
            )
        ),
    CONSTRAINT cleanup_tombstones_due_check
        CHECK (next_attempt_at >= delete_not_before),
    CONSTRAINT cleanup_tombstones_reservation_pair_check
        CHECK (
            (reservation_token IS NULL) = (reservation_expires_at IS NULL)
        ),
    CONSTRAINT cleanup_tombstones_attempt_pair_check
        CHECK ((last_attempt_at IS NULL) = (last_result IS NULL)),
    CONSTRAINT cleanup_tombstones_attempt_shape_check
        CHECK (
            (
                last_result IS NULL
                AND failure_streak = 0
            )
            OR
            (
                last_result IS NOT NULL
                AND last_result IN ('delete_succeeded', 'confirmed_absent')
                AND failure_streak = 0
            )
            OR
            (
                last_result IS NOT NULL
                AND last_result IN (
                    'transient_error',
                    'ambiguous_error',
                    'auth_error',
                    'configuration_error',
                    'other_deterministic_error'
                )
                AND failure_streak > 0
            )
        )
);

CREATE INDEX uploads_pending_due_idx
    ON uploads (upload_deadline, upload_id)
    WHERE state = 'pending';

CREATE INDEX uploads_finalizing_due_idx
    ON uploads (reconcile_after, upload_id)
    WHERE state = 'finalizing';

CREATE INDEX cleanup_tombstones_due_idx
    ON cleanup_tombstones (next_attempt_at, object_key);

INSERT INTO schema_migrations (version, applied_at)
VALUES (1, clock_timestamp());
