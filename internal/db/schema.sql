CREATE TABLE IF NOT EXISTS jobs (
    id                 uuid PRIMARY KEY,
    url                text NOT NULL,
    state              text NOT NULL,
    title              text,
    site               text,
    stream_key         text,
    percent            double precision NOT NULL DEFAULT 0,
    bytes_downloaded   bigint NOT NULL DEFAULT 0,
    bytes_total        bigint NOT NULL DEFAULT 0,
    output_object_key  text,
    output_filename    text,
    error              text,
    playlist           boolean NOT NULL DEFAULT false,
    cookie             text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs (state);
CREATE INDEX IF NOT EXISTS jobs_created_at_idx ON jobs (created_at DESC);
