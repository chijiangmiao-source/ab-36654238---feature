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
//   - batch_operations: idempotency ledger for multi-equipment publishes,
//     keyed by the global operation id alone.
//
// Every publish runs in a single transaction that locks the equipment's head
// row with SELECT ... FOR UPDATE. The lock serializes publishers per
// equipment, so two publishes racing on the same seen revision cannot both
// succeed, and a failed transaction leaves no partial split behind. Batch
// publishes lock every involved head row in ascending equipment order — the
// same global order single publishes follow — so overlapping publishes
// compete uniformly, cannot deadlock, and the whole package commits or rolls
// back as one.
package store

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"slices"

	"github.com/lib/pq"

	"github.com/example/calibsvc/internal/split"
)

// Stable domain errors, mapped to HTTP status codes by the API layer.
var (
	ErrStaleRevision     = errors.New("seen revision does not match the current revision")
	ErrOperationConflict = errors.New("operation id already used with different parameters")
	ErrEquipmentNotFound = errors.New("equipment not found")
	ErrRevisionNotFound  = errors.New("revision not found")
	ErrBatchNotCovered   = errors.New("batch not covered by any calibration segment")
	// ErrBatchInvalid is returned when a batch publish package is rejected
	// wholesale (empty package, duplicate equipment, or a per-equipment
	// validation failure identified by Equipment). The error message names
	// the first failing equipment stably.
	ErrBatchInvalid = errors.New("batch publish rejected")
)

// BatchItemError wraps a per-equipment failure inside a batch publish so the
// caller can locate the first failing equipment by its stable identifier.
type BatchItemError struct {
	Equipment string
	Err       error
}

func (e *BatchItemError) Error() string {
	return fmt.Sprintf("equipment %q: %v", e.Equipment, e.Err)
}

func (e *BatchItemError) Unwrap() error { return e.Err }

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

-- Global idempotency ledger for multi-equipment batch publishes. One row per
-- global operation id; response holds the exact first response for replay.
CREATE TABLE IF NOT EXISTS batch_operations (
    operation_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response     TEXT NOT NULL
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
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'batch_operations_immutable') THEN
        CREATE TRIGGER batch_operations_immutable BEFORE UPDATE OR DELETE ON batch_operations
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

// BatchItem is one equipment's part of a multi-equipment publish.
type BatchItem struct {
	Equipment    string
	SeenRevision int64
	Lower        int64
	Upper        int64
	Content      string // canonical JSON
}

// BatchParams is one validated, indivisible batch publish package.
type BatchParams struct {
	OperationID string // global idempotency key
	Items       []BatchItem
}

// BatchItemResult is one equipment's independently incremented revision with
// its full effective-interval snapshot.
type BatchItemResult struct {
	Equipment string
	Revision  int64
	Segments  []split.Segment
}

// BatchResult is the outcome of a successful (or replayed) batch publish.
type BatchResult struct {
	OperationID string
	Results     []BatchItemResult
	Response    []byte // exact response body, stored for idempotent replay
	Replayed    bool   // true when the result comes from the global ledger
}

// BatchPublish applies one publish to each of several equipments as a single
// indivisible transaction.
//
// All involved head rows are locked in ascending equipment order before any
// validation or write. That is the same global order single publishes follow
// (each of them locks its single head row), so batch and single publishes
// overlapping on an equipment compete uniformly: no deadlock is possible and
// a failed item rolls back every equipment in the package.
//
// Item failures are returned as *BatchItemError naming the first failing
// equipment; items are processed (and reported) in ascending equipment order,
// so that equipment is stable across retries.
//
// The global operation id has its own ledger: retrying with the byte-identical
// package replays the first full result; any parameter change is rejected with
// ErrOperationConflict. Per-equipment ledger rows (keyed by the same global
// operation id) keep the single endpoint's idempotency semantics consistent.
func (s *Store) BatchPublish(ctx context.Context, p BatchParams) (*BatchResult, error) {
	// Defensive copy sorted into the global lock order; the API layer
	// validates and sorts too, but the store must not depend on caller order
	// for deadlock safety or stable failure location.
	items := append([]BatchItem(nil), p.Items...)
	slices.SortFunc(items, func(a, b BatchItem) int {
		return cmp.Compare(a.Equipment, b.Equipment)
	})
	if len(items) == 0 {
		return nil, fmt.Errorf("%w: package must contain at least one item", ErrBatchInvalid)
	}
	for i := 1; i < len(items); i++ {
		if items[i].Equipment == items[i-1].Equipment {
			return nil, &BatchItemError{
				Equipment: items[i].Equipment,
				Err:       fmt.Errorf("%w: duplicate equipment in package", ErrBatchInvalid),
			}
		}
	}
	globalHash := batchRequestHash(p.OperationID, items)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // no-op after Commit

	// Lock every involved head row in ascending equipment order. Creating
	// missing rows first means the lock itself never blocks on insertion;
	// rows created here disappear with the transaction on failure.
	heads := make(map[string]int64, len(items))
	for _, it := range items {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO heads (equipment, revision) VALUES ($1, 0)
			 ON CONFLICT (equipment) DO NOTHING`, it.Equipment); err != nil {
			return nil, err
		}
		var head int64
		if err := tx.QueryRowContext(ctx,
			`SELECT revision FROM heads WHERE equipment = $1 FOR UPDATE`, it.Equipment,
		).Scan(&head); err != nil {
			return nil, err
		}
		heads[it.Equipment] = head
	}

	// Global idempotency ledger, checked only after all locks are held so
	// identical retries serialize against the first attempt even before its
	// commit.
	var existingHash string
	var existingResponse []byte
	switch err := tx.QueryRowContext(ctx,
		`SELECT request_hash, response FROM batch_operations WHERE operation_id = $1`,
		p.OperationID,
	).Scan(&existingHash, &existingResponse); {
	case err == nil:
		if existingHash != globalHash {
			return nil, ErrOperationConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &BatchResult{
			OperationID: p.OperationID,
			Response:    existingResponse,
			Replayed:    true,
		}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}

	results := make([]BatchItemResult, 0, len(items))
	for _, it := range items {
		itemHash := requestHash(it.Equipment, p.OperationID, it.SeenRevision,
			it.Lower, it.Upper, it.Content)

		// Per-equipment ledger: shares the single publish namespace. An exact
		// match (e.g. the single endpoint already applied this operation to
		// this equipment) replays that revision inside the package; a
		// mismatch conflicts before the seen-revision check, exactly as in
		// Publish.
		var ledgerHash string
		var ledgerRevision int64
		switch err := tx.QueryRowContext(ctx,
			`SELECT request_hash, revision FROM operations
			 WHERE equipment = $1 AND operation_id = $2`,
			it.Equipment, p.OperationID,
		).Scan(&ledgerHash, &ledgerRevision); {
		case err == nil:
			if ledgerHash != itemHash {
				return nil, &BatchItemError{Equipment: it.Equipment, Err: ErrOperationConflict}
			}
			segs, err := loadSegments(ctx, tx, it.Equipment, ledgerRevision)
			if err != nil {
				return nil, err
			}
			results = append(results, BatchItemResult{
				Equipment: it.Equipment,
				Revision:  ledgerRevision,
				Segments:  segs,
			})
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return nil, err
		}

		// Optimistic concurrency against the head as advanced by earlier
		// items in this same package.
		head := heads[it.Equipment]
		if it.SeenRevision != head {
			return nil, &BatchItemError{Equipment: it.Equipment, Err: ErrStaleRevision}
		}

		current, err := loadSegments(ctx, tx, it.Equipment, head)
		if err != nil {
			return nil, err
		}
		next := split.Apply(current, it.Lower, it.Upper, it.Content)
		newRevision := head + 1

		for _, sg := range next {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO segments (equipment, revision, lower, upper, content)
				 VALUES ($1, $2, $3, $4, $5)`,
				it.Equipment, newRevision, sg.Lower, sg.Upper, sg.Content); err != nil {
				return nil, err
			}
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE heads SET revision = $2 WHERE equipment = $1`,
			it.Equipment, newRevision); err != nil {
			return nil, err
		}

		// Record under the shared per-equipment ledger so the single
		// endpoint replays/conflicts consistently for this operation id.
		itemResponse := marshalPublishResponse(it.Equipment, newRevision, next)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO operations (equipment, operation_id, request_hash, revision, response)
			 VALUES ($1, $2, $3, $4, $5)`,
			it.Equipment, p.OperationID, itemHash, newRevision, itemResponse); err != nil {
			if isUniqueViolation(err) {
				return nil, &BatchItemError{Equipment: it.Equipment, Err: ErrOperationConflict}
			}
			return nil, err
		}

		heads[it.Equipment] = newRevision
		results = append(results, BatchItemResult{
			Equipment: it.Equipment,
			Revision:  newRevision,
			Segments:  next,
		})
	}

	response := marshalBatchResponse(p.OperationID, results)
	// Last step: two same-id packages over disjoint equipment sets cannot
	// serialize on head locks; the ledger primary key is their arbiter. The
	// insert waits for an in-flight conflicting transaction to resolve.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO batch_operations (operation_id, request_hash, response)
		 VALUES ($1, $2, $3)`,
		p.OperationID, globalHash, response); err != nil {
		if isUniqueViolation(err) {
			// A concurrent package committed the same operation id first;
			// this transaction is now aborted. Roll it back and answer from
			// the committed ledger row.
			_ = tx.Rollback()
			return s.replayBatchLedger(ctx, p.OperationID, globalHash)
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &BatchResult{
		OperationID: p.OperationID,
		Results:     results,
		Response:    response,
	}, nil
}

// replayBatchLedger answers a batch publish from the committed global ledger
// after losing the insert race: the identical package replays the first
// complete result, a different one is rejected.
func (s *Store) replayBatchLedger(ctx context.Context, operationID, requestHash string) (*BatchResult, error) {
	var existingHash string
	var response []byte
	if err := s.db.QueryRowContext(ctx,
		`SELECT request_hash, response FROM batch_operations WHERE operation_id = $1`,
		operationID,
	).Scan(&existingHash, &response); err != nil {
		return nil, err
	}
	if existingHash != requestHash {
		return nil, ErrOperationConflict
	}
	return &BatchResult{
		OperationID: operationID,
		Response:    response,
		Replayed:    true,
	}, nil
}

// RequestHash binds an operation id to its full parameter set so that reused
// ids with different parameters are detected deterministically. The single
// and batch publish paths share it, so an operation id means the same thing
// on both endpoints.
func RequestHash(equipment, operationID string, seenRevision, lower, upper int64, content string) string {
	h := newRequestHasher()
	fmt.Fprintf(h, "%s\x00%s\x00%d\x00%d\x00%d\x00%s",
		equipment, operationID, seenRevision, lower, upper, content)
	return hex.EncodeToString(h.Sum(nil))
}

// requestHash is the unexported twin used inside the store.
func requestHash(equipment, operationID string, seenRevision, lower, upper int64, content string) string {
	return RequestHash(equipment, operationID, seenRevision, lower, upper, content)
}

func newRequestHasher() hash.Hash { return sha256.New() }

// batchRequestHash binds the global operation id to the complete,
// order-independent package: the same equipments with the same parameters
// hash equally in any request order, while any added/dropped equipment or
// changed parameter changes the hash.
func batchRequestHash(operationID string, items []BatchItem) string {
	h := newRequestHasher()
	h.Write([]byte(operationID))
	for _, it := range items { // already sorted by equipment
		fmt.Fprintf(h, "\x00%s\x00%d\x00%d\x00%d\x00%s",
			it.Equipment, it.SeenRevision, it.Lower, it.Upper, it.Content)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}

func marshalBatchResponse(operationID string, results []BatchItemResult) []byte {
	type itemJSON struct {
		Equipment string        `json:"equipment"`
		Revision  int64         `json:"revision"`
		Segments  []segmentBody `json:"segments"`
	}
	type responseJSON struct {
		OperationID string     `json:"operation_id"`
		Results     []itemJSON `json:"results"`
	}
	resp := responseJSON{
		OperationID: operationID,
		Results:     make([]itemJSON, 0, len(results)),
	}
	for _, r := range results {
		resp.Results = append(resp.Results, itemJSON{
			Equipment: r.Equipment,
			Revision:  r.Revision,
			Segments:  segmentBodies(r.Segments),
		})
	}
	b, _ := json.Marshal(resp) // cannot fail: all fields are marshalable
	return b
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

// segmentBody is the JSON wire shape of one segment in publish responses.
type segmentBody struct {
	Lower   int64           `json:"lower"`
	Upper   int64           `json:"upper"`
	Content json.RawMessage `json:"content"`
}

func segmentBodies(segs []split.Segment) []segmentBody {
	out := make([]segmentBody, 0, len(segs))
	for _, sg := range segs {
		out = append(out, segmentBody{
			Lower:   sg.Lower,
			Upper:   sg.Upper,
			Content: json.RawMessage(sg.Content),
		})
	}
	return out
}

func marshalPublishResponse(equipment string, revision int64, segs []split.Segment) []byte {
	type responseJSON struct {
		Equipment string        `json:"equipment"`
		Revision  int64         `json:"revision"`
		Segments  []segmentBody `json:"segments"`
	}
	resp := responseJSON{
		Equipment: equipment,
		Revision:  revision,
		Segments:  segmentBodies(segs),
	}
	b, _ := json.Marshal(resp) // cannot fail: all fields are marshalable
	return b
}
