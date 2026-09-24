// Package store persists calibration revisions in PostgreSQL.
//
// Data model:
//
//   - heads:      one row per equipment, pointing at the current revision.
//   - segments:   append-only snapshot of the full segment list of every
//     revision; rows are never updated or deleted (enforced by trigger), so
//     every historical revision stays recomputable.
//   - operations: idempotency ledger mapping (equipment, operation_id) to the
//     request hash and the stored first response.
//
// Every publish runs in a single transaction that locks the equipment's head
// row with SELECT ... FOR UPDATE. The lock serializes publishers per
// equipment, so two publishes racing on the same seen revision cannot both
// succeed, and a failed transaction leaves no partial split behind.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/example/calibsvc/internal/split"
)

// Stable domain errors, mapped to HTTP status codes by the API layer.
var (
	ErrStaleRevision     = errors.New("seen revision does not match the current revision")
	ErrOperationConflict = errors.New("operation id already used with different parameters")
	ErrEquipmentNotFound = errors.New("equipment not found")
	ErrRevisionNotFound  = errors.New("revision not found")
	ErrBatchNotCovered   = errors.New("batch not covered by any calibration segment")
)

// Store provides transactional access to the calibration database.
type Store struct {
	db *sql.DB
}

// New returns a Store backed by db.
func New(db *sql.DB) *Store { return &Store{db: db} }

// Ping reports whether the database is reachable (used by /health).
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Migrate creates the schema if it does not exist yet. It is idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

const schema = `
CREATE TABLE IF NOT EXISTS heads (
    equipment TEXT PRIMARY KEY,
    revision BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    equipment TEXT NOT NULL,
    revision BIGINT NOT NULL,
    lower    BIGINT NOT NULL,
    upper    BIGINT NOT NULL,
    content  TEXT NOT NULL,
    PRIMARY KEY (equipment, revision, lower),
    CHECK (lower < upper)
);

CREATE TABLE IF NOT EXISTS operations (
    equipment    TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    revision     BIGINT NOT NULL,
    response     TEXT NOT NULL,
    PRIMARY KEY (equipment, operation_id)
);

-- Historical revisions and the idempotency ledger are immutable: any attempt
-- to rewrite or delete them is rejected at the database level.
CREATE OR REPLACE FUNCTION reject_history_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'history is immutable: % on % is not allowed', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'segments_immutable') THEN
        CREATE TRIGGER segments_immutable BEFORE UPDATE OR DELETE ON segments
            FOR EACH ROW EXECUTE FUNCTION reject_history_mutation();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operations_immutable') THEN
        CREATE TRIGGER operations_immutable BEFORE UPDATE OR DELETE ON operations
            FOR EACH ROW EXECUTE FUNCTION reject_history_mutation();
    END IF;
END $$;
`

// PublishParams describes one validated publish call.
type PublishParams struct {
	Equipment    string
	OperationID  string
	SeenRevision int64
	Lower        int64
	Upper        int64
	Content      string // canonical JSON
	RequestHash  string // hash over all request parameters
}

// PublishResult is the outcome of a successful (or replayed) publish.
type PublishResult struct {
	Equipment string
	Revision  int64
	Segments  []split.Segment
	Response  []byte // exact response body, stored for idempotent replay
	Replayed  bool   // true when the result comes from the idempotency ledger
}

// Publish atomically validates and applies a publish call.
//
// Ordering inside the transaction matters: the idempotency ledger is checked
// before the revision check so that a legitimate retry of an already applied
// operation replays its first result instead of failing with a stale
// revision error.
func (s *Store) Publish(ctx context.Context, p PublishParams) (*PublishResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // no-op after Commit

	// Create the head row on first contact, then lock it. Concurrent
	// publishers for the same equipment serialize on this row lock.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO heads (equipment, revision) VALUES ($1, 0)
		 ON CONFLICT (equipment) DO NOTHING`, p.Equipment); err != nil {
		return nil, err
	}
	var head int64
	if err := tx.QueryRowContext(ctx,
		`SELECT revision FROM heads WHERE equipment = $1 FOR UPDATE`, p.Equipment,
	).Scan(&head); err != nil {
		return nil, err
	}

	// Idempotency: same operation id replays the first result if the
	// parameters match, and is rejected as a conflict if they do not.
	var (
		existingHash     string
		existingRevision int64
		existingResponse []byte
	)
	err = tx.QueryRowContext(ctx,
		`SELECT request_hash, revision, response FROM operations
		 WHERE equipment = $1 AND operation_id = $2`,
		p.Equipment, p.OperationID,
	).Scan(&existingHash, &existingRevision, &existingResponse)
	switch {
	case err == nil:
		if existingHash != p.RequestHash {
			return nil, ErrOperationConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &PublishResult{
			Equipment: p.Equipment,
			Revision:  existingRevision,
			Response:  existingResponse,
			Replayed:  true,
		}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}

	// Optimistic concurrency: the caller must have seen the current head.
	if p.SeenRevision != head {
		return nil, ErrStaleRevision
	}

	current, err := loadSegments(ctx, tx, p.Equipment, head)
	if err != nil {
		return nil, err
	}
	next := split.Apply(current, p.Lower, p.Upper, p.Content)
	newRevision := head + 1

	for _, sg := range next {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO segments (equipment, revision, lower, upper, content)
			 VALUES ($1, $2, $3, $4, $5)`,
			p.Equipment, newRevision, sg.Lower, sg.Upper, sg.Content); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE heads SET revision = $2 WHERE equipment = $1`,
		p.Equipment, newRevision); err != nil {
		return nil, err
	}

	response := marshalPublishResponse(p.Equipment, newRevision, next)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO operations (equipment, operation_id, request_hash, revision, response)
		 VALUES ($1, $2, $3, $4, $5)`,
		p.Equipment, p.OperationID, p.RequestHash, newRevision, response); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &PublishResult{
		Equipment: p.Equipment,
		Revision:  newRevision,
		Segments:  next,
		Response:  response,
	}, nil
}

// QueryResult is the unique segment effective for a batch at a revision.
type QueryResult struct {
	Equipment string
	Batch     int64
	Revision  int64
	Lower     int64
	Upper     int64
	Content   string // canonical JSON
}

// Query returns the segment covering batch at the given revision, or at the
// current head revision when revision is nil.
func (s *Store) Query(ctx context.Context, equipment string, batch int64, revision *int64) (*QueryResult, error) {
	var head int64
	err := s.db.QueryRowContext(ctx,
		`SELECT revision FROM heads WHERE equipment = $1`, equipment,
	).Scan(&head)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrEquipmentNotFound
	}
	if err != nil {
		return nil, err
	}

	rev := head
	if revision != nil {
		if *revision < 1 || *revision > head {
			return nil, ErrRevisionNotFound
		}
		rev = *revision
	}

	var r QueryResult
	err = s.db.QueryRowContext(ctx,
		`SELECT lower, upper, content FROM segments
		 WHERE equipment = $1 AND revision = $2 AND lower <= $3 AND $3 < upper`,
		equipment, rev, batch,
	).Scan(&r.Lower, &r.Upper, &r.Content)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBatchNotCovered
	}
	if err != nil {
		return nil, err
	}
	r.Equipment = equipment
	r.Batch = batch
	r.Revision = rev
	return &r, nil
}

func loadSegments(ctx context.Context, tx *sql.Tx, equipment string, revision int64) ([]split.Segment, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT lower, upper, content FROM segments
		 WHERE equipment = $1 AND revision = $2 ORDER BY lower`,
		equipment, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []split.Segment
	for rows.Next() {
		var sg split.Segment
		if err := rows.Scan(&sg.Lower, &sg.Upper, &sg.Content); err != nil {
			return nil, err
		}
		out = append(out, sg)
	}
	return out, rows.Err()
}

func marshalPublishResponse(equipment string, revision int64, segs []split.Segment) []byte {
	type segmentJSON struct {
		Lower   int64           `json:"lower"`
		Upper   int64           `json:"upper"`
		Content json.RawMessage `json:"content"`
	}
	type responseJSON struct {
		Equipment string        `json:"equipment"`
		Revision  int64         `json:"revision"`
		Segments  []segmentJSON `json:"segments"`
	}
	resp := responseJSON{
		Equipment: equipment,
		Revision:  revision,
		Segments:  make([]segmentJSON, 0, len(segs)),
	}
	for _, sg := range segs {
		resp.Segments = append(resp.Segments, segmentJSON{
			Lower:   sg.Lower,
			Upper:   sg.Upper,
			Content: json.RawMessage(sg.Content),
		})
	}
	b, _ := json.Marshal(resp) // cannot fail: all fields are marshalable
	return b
}
