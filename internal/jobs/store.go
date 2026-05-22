package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("job not found")

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const jobColumns = `id, url, state, title, site, stream_key, percent,
	bytes_downloaded, bytes_total, output_object_key, output_filename,
	error, playlist, cookie, created_at, updated_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var (
		title, site, streamKey, objKey, outFile, errStr, cookie *string
	)
	err := row.Scan(
		&j.ID, &j.URL, &j.State, &title, &site, &streamKey,
		&j.Percent, &j.BytesDownloaded, &j.BytesTotal,
		&objKey, &outFile, &errStr, &j.Playlist, &cookie,
		&j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return j, err
	}
	if title != nil {
		j.Title = *title
	}
	if site != nil {
		j.Site = *site
	}
	if streamKey != nil {
		j.StreamKey = *streamKey
	}
	if objKey != nil {
		j.OutputObjectKey = *objKey
	}
	if outFile != nil {
		j.OutputFilename = *outFile
	}
	if errStr != nil {
		j.Error = *errStr
	}
	if cookie != nil {
		j.Cookie = *cookie
	}
	return j, nil
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (s *Store) Create(ctx context.Context, req Request, title, site, streamKey string, bytesTotal int64) (Job, error) {
	now := time.Now().UTC()
	j := Job{
		ID:         uuid.NewString(),
		URL:        req.URL,
		State:      StateQueued,
		Title:      title,
		Site:       site,
		StreamKey:  streamKey,
		BytesTotal: bytesTotal,
		Playlist:   req.Playlist,
		Cookie:     req.Cookie,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO jobs (`+jobColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		j.ID, j.URL, j.State, nullable(j.Title), nullable(j.Site), nullable(j.StreamKey),
		j.Percent, j.BytesDownloaded, j.BytesTotal,
		nullable(j.OutputObjectKey), nullable(j.OutputFilename), nullable(j.Error),
		j.Playlist, nullable(j.Cookie),
		j.CreatedAt, j.UpdatedAt,
	)
	if err != nil {
		return Job{}, fmt.Errorf("insert job: %w", err)
	}
	return j, nil
}

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

func (s *Store) List(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs ORDER BY created_at DESC LIMIT 200`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	out := make([]Job, 0)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	return nil
}

// Update applies fn to a job snapshot and persists changed columns.
// The whole row is rewritten — simpler than per-field updates and the
// row is small.
func (s *Store) Update(ctx context.Context, id string, fn func(*Job)) (Job, error) {
	j, err := s.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	fn(&j)
	j.UpdatedAt = time.Now().UTC()
	_, err = s.pool.Exec(ctx, `UPDATE jobs SET
		state=$2, title=$3, site=$4, stream_key=$5, percent=$6,
		bytes_downloaded=$7, bytes_total=$8, output_object_key=$9,
		output_filename=$10, error=$11, playlist=$12, cookie=$13, updated_at=$14
		WHERE id=$1`,
		j.ID, j.State, nullable(j.Title), nullable(j.Site), nullable(j.StreamKey),
		j.Percent, j.BytesDownloaded, j.BytesTotal,
		nullable(j.OutputObjectKey), nullable(j.OutputFilename), nullable(j.Error),
		j.Playlist, nullable(j.Cookie), j.UpdatedAt,
	)
	if err != nil {
		return Job{}, fmt.Errorf("update job: %w", err)
	}
	return j, nil
}
