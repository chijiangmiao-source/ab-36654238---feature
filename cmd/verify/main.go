// Command verify is the one-shot acceptance harness for the calibration
// service. It runs build checks and unit tests against the source tree, then
// exercises a running service over HTTP (interval splitting, history
// immutability, idempotent replay, concurrent stale-revision conflicts and
// error stability), and finally exits 0 when everything passed and 1
// otherwise.
//
// Configuration is via environment:
//
//	APP_URL       base URL of the service (default http://localhost:8080)
//	DATABASE_URL  optional PostgreSQL DSN enabling the DB-level
//	              history-immutability probe
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

var (
	appURL = getenv("APP_URL", "http://localhost:8080")
	dbURL  = os.Getenv("DATABASE_URL")

	runID  = time.Now().UnixNano()
	opSeq  atomic.Int64
	failed atomic.Int32

	// splitEq carries state between the splitting and history scenarios.
	splitEq string
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("[verify] ")

	step("build check: go build ./...", func() error { return run("go", "build", "-buildvcs=false", "./...") })
	step("build check: go vet ./...", func() error { return run("go", "vet", "./...") })
	step("unit tests: go test ./...", func() error { return run("go", "test", "-count=1", "./...") })

	step("service becomes healthy", waitHealthy)
	step("HTTP smoke: interval splitting and coverage", scenarioSplit)
	step("HTTP smoke: history immutability via API", scenarioHistory)
	step("HTTP smoke: idempotent replay and operation conflict", scenarioIdempotency)
	step("HTTP smoke: concurrent stale-revision conflict", scenarioConcurrency)
	step("HTTP smoke: batch publish across equipments", scenarioBatchPublish)
	step("HTTP smoke: batch publish rolls back as a whole", scenarioBatchRollback)
	step("HTTP smoke: batch idempotent replay and conflict", scenarioBatchIdempotency)
	step("HTTP smoke: batch interleaved with single publishes", scenarioBatchInterleave)
	step("HTTP smoke: validation and stable errors", scenarioValidation)
	if dbURL != "" {
		step("history immutability enforced by database", scenarioDBImmutability)
	}

	if failed.Load() > 0 {
		log.Printf("VERIFY FAILED (%d failing step(s))", failed.Load())
		os.Exit(1)
	}
	log.Print("VERIFY PASSED")
}

func step(name string, fn func() error) {
	if err := fn(); err != nil {
		failed.Add(1)
		log.Printf("FAIL: %s: %v", name, err)
		return
	}
	log.Printf("OK:   %s", name)
}

func run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func uniqueEq(prefix string) string { return fmt.Sprintf("%s-%d", prefix, runID) }

func uniqueOp(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, runID, opSeq.Add(1))
}

// ---------------------------------------------------------------------------
// HTTP client helpers
// ---------------------------------------------------------------------------

var hc = &http.Client{Timeout: 10 * time.Second}

func waitHealthy() error {
	deadline := time.Now().Add(90 * time.Second)
	for {
		resp, err := hc.Get(appURL + "/health")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("service at %s did not become healthy within 90s", appURL)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func doJSON(method, rawURL, body string) (int, []byte, error) {
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, rawURL, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, b, nil
}

func publish(eq, op string, seen, lower, upper int64, content string) (int, []byte, error) {
	body := fmt.Sprintf(
		`{"operation_id":%q,"seen_revision":%d,"lower":%d,"upper":%d,"content":%s}`,
		op, seen, lower, upper, content)
	return doJSON(http.MethodPost,
		fmt.Sprintf("%s/v1/equipments/%s/calibrations", appURL, url.PathEscape(eq)), body)
}

func publishRaw(eq, body string) (int, []byte, error) {
	return doJSON(http.MethodPost,
		fmt.Sprintf("%s/v1/equipments/%s/calibrations", appURL, url.PathEscape(eq)), body)
}

// batchItem is one equipment entry of a batch publish package.
type batchItem struct {
	Equipment    string `json:"equipment"`
	SeenRevision int64  `json:"seen_revision"`
	Lower        int64  `json:"lower"`
	Upper        int64  `json:"upper"`
	Content      string `json:"content"`
}

// batchPublish posts one indivisible multi-equipment package. The items are
// serialized in the order given so tests control the on-the-wire order.
func batchPublish(op string, items ...batchItem) (int, []byte, error) {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{"operation_id":%q,"items":[`, op)
	for i, it := range items {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"equipment":%q,"seen_revision":%d,"lower":%d,"upper":%d,"content":%s}`,
			it.Equipment, it.SeenRevision, it.Lower, it.Upper, it.Content)
	}
	sb.WriteString(`]}`)
	return doJSON(http.MethodPost, appURL+"/v1/calibrations:batch", sb.String())
}

func batchPublishRaw(body string) (int, []byte, error) {
	return doJSON(http.MethodPost, appURL+"/v1/calibrations:batch", body)
}

func query(eq string, batch int64, revision string) (int, []byte, error) {
	u := fmt.Sprintf("%s/v1/equipments/%s/calibration?batch=%d",
		appURL, url.PathEscape(eq), batch)
	if revision != "" {
		u += "&revision=" + revision
	}
	return doJSON(http.MethodGet, u, "")
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

type segment struct {
	Lower   int64           `json:"lower"`
	Upper   int64           `json:"upper"`
	Content json.RawMessage `json:"content"`
}

type publishResponse struct {
	Equipment string    `json:"equipment"`
	Revision  int64     `json:"revision"`
	Segments  []segment `json:"segments"`
}

type batchItemResult struct {
	Equipment string    `json:"equipment"`
	Revision  int64     `json:"revision"`
	Segments  []segment `json:"segments"`
}

type batchResponse struct {
	OperationID string            `json:"operation_id"`
	Results     []batchItemResult `json:"results"`
}

type queryResponse struct {
	Equipment string          `json:"equipment"`
	Batch     int64           `json:"batch"`
	Revision  int64           `json:"revision"`
	Lower     int64           `json:"lower"`
	Upper     int64           `json:"upper"`
	Content   json.RawMessage `json:"content"`
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func seg(l, u int64, content string) segment {
	return segment{Lower: l, Upper: u, Content: json.RawMessage(content)}
}

func expectPublish(eq, op string, seen, lower, upper int64, content string, wantRev int64, want ...segment) error {
	st, body, err := publish(eq, op, seen, lower, upper, content)
	if err != nil {
		return err
	}
	if st != http.StatusCreated {
		return fmt.Errorf("publish %s: status %d, want 201 (body %s)", op, st, body)
	}
	var pr publishResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return fmt.Errorf("publish %s: response not JSON: %v (%s)", op, err, body)
	}
	if pr.Equipment != eq {
		return fmt.Errorf("publish %s: equipment = %q, want %q", op, pr.Equipment, eq)
	}
	if pr.Revision != wantRev {
		return fmt.Errorf("publish %s: revision = %d, want %d", op, pr.Revision, wantRev)
	}
	return expectSegments(pr.Segments, want)
}

func expectSegments(got []segment, want []segment) error {
	if len(got) != len(want) {
		return fmt.Errorf("segments = %s, want %s", mustJSON(got), mustJSON(want))
	}
	for i := range want {
		if got[i].Lower != want[i].Lower || got[i].Upper != want[i].Upper ||
			string(got[i].Content) != string(want[i].Content) {
			return fmt.Errorf("segments = %s, want %s", mustJSON(got), mustJSON(want))
		}
	}
	return nil
}

func expectQuery(eq string, batch int64, revision string, wantRev, wantLower, wantUpper int64, wantContent string) error {
	st, body, err := query(eq, batch, revision)
	if err != nil {
		return err
	}
	if st != http.StatusOK {
		return fmt.Errorf("query %s batch=%d rev=%q: status %d, want 200 (body %s)",
			eq, batch, revision, st, body)
	}
	var qr queryResponse
	if err := json.Unmarshal(body, &qr); err != nil {
		return fmt.Errorf("query %s batch=%d rev=%q: response not JSON: %v (%s)",
			eq, batch, revision, err, body)
	}
	if qr.Equipment != eq || qr.Batch != batch {
		return fmt.Errorf("query echo mismatch: %s", body)
	}
	if qr.Revision != wantRev || qr.Lower != wantLower || qr.Upper != wantUpper ||
		string(qr.Content) != wantContent {
		return fmt.Errorf("query %s batch=%d rev=%q: got rev=%d [%d,%d) %s, want rev=%d [%d,%d) %s",
			eq, batch, revision, qr.Revision, qr.Lower, qr.Upper, qr.Content,
			wantRev, wantLower, wantUpper, wantContent)
	}
	return nil
}

func expectError(st int, body []byte, wantStatus int, wantCode string) error {
	if st != wantStatus {
		return fmt.Errorf("status %d, want %d (body %s)", st, wantStatus, body)
	}
	var er errorResponse
	if err := json.Unmarshal(body, &er); err != nil {
		return fmt.Errorf("error body not JSON: %v (%s)", err, body)
	}
	if er.Error.Code != wantCode {
		return fmt.Errorf("error code = %q, want %q (body %s)", er.Error.Code, wantCode, body)
	}
	return nil
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<unmarshalable: %v>", err)
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

// scenarioSplit publishes overlapping intervals and checks that the overlap
// is replaced, residuals are kept, equal-content neighbours merge, and
// covered/uncovered batches resolve correctly.
func scenarioSplit() error {
	splitEq = uniqueEq("EQ-SPLIT")
	eq := splitEq

	if err := expectPublish(eq, uniqueOp("s1"), 0, 0, 100, `"A"`, 1,
		seg(0, 100, `"A"`)); err != nil {
		return err
	}
	if err := expectPublish(eq, uniqueOp("s2"), 1, 100, 200, `"B"`, 2,
		seg(0, 100, `"A"`), seg(100, 200, `"B"`)); err != nil {
		return err
	}
	// Backfill the middle batch range: must split both neighbours without
	// touching the still-valid outer parts.
	if err := expectPublish(eq, uniqueOp("s3"), 2, 50, 150, `"C"`, 3,
		seg(0, 50, `"A"`), seg(50, 150, `"C"`), seg(150, 200, `"B"`)); err != nil {
		return err
	}

	// Coverage at the head revision.
	for _, tc := range []struct {
		batch        int64
		lower, upper int64
		content      string
	}{
		{0, 0, 50, `"A"`},
		{49, 0, 50, `"A"`},
		{50, 50, 150, `"C"`},
		{149, 50, 150, `"C"`},
		{150, 150, 200, `"B"`},
		{199, 150, 200, `"B"`},
	} {
		if err := expectQuery(eq, tc.batch, "", 3, tc.lower, tc.upper, tc.content); err != nil {
			return err
		}
	}
	// Uncovered batches give a stable error.
	for _, batch := range []int64{-1, 200, 1 << 40} {
		st, body, err := query(eq, batch, "")
		if err != nil {
			return err
		}
		if err := expectError(st, body, http.StatusNotFound, "BATCH_NOT_COVERED"); err != nil {
			return fmt.Errorf("batch %d: %w", batch, err)
		}
	}

	// Adjacent publish with identical content merges with the neighbour.
	if err := expectPublish(eq, uniqueOp("s4"), 3, 200, 300, `"B"`, 4,
		seg(0, 50, `"A"`), seg(50, 150, `"C"`), seg(150, 300, `"B"`)); err != nil {
		return err
	}
	if err := expectQuery(eq, 250, "", 4, 150, 300, `"B"`); err != nil {
		return err
	}

	// Republishing a range with the neighbour's content merges across.
	if err := expectPublish(eq, uniqueOp("s5"), 4, 0, 50, `"C"`, 5,
		seg(0, 150, `"C"`), seg(150, 300, `"B"`)); err != nil {
		return err
	}
	return expectQuery(eq, 10, "", 5, 0, 150, `"C"`)
}

// scenarioHistory re-queries the equipment mutated by scenarioSplit at older
// revisions and checks that the historical views are unchanged.
func scenarioHistory() error {
	eq := splitEq
	if eq == "" {
		return fmt.Errorf("split scenario did not run")
	}
	checks := []struct {
		batch        int64
		revision     string
		wantRev      int64
		lower, upper int64
		content      string
	}{
		{75, "1", 1, 0, 100, `"A"`},  // before the backfill
		{75, "2", 2, 0, 100, `"A"`},  // still untouched by [100,200)
		{75, "3", 3, 50, 150, `"C"`}, // backfill effective
		{120, "2", 2, 100, 200, `"B"`},
		{10, "4", 4, 0, 50, `"A"`},  // before the merge publish
		{10, "5", 5, 0, 150, `"C"`}, // after the merge publish
		{250, "4", 4, 150, 300, `"B"`},
	}
	for _, tc := range checks {
		if err := expectQuery(eq, tc.batch, tc.revision, tc.wantRev, tc.lower, tc.upper, tc.content); err != nil {
			return err
		}
	}
	// A batch uncovered at an old revision stays uncovered there.
	st, body, err := query(eq, 250, "3")
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusNotFound, "BATCH_NOT_COVERED"); err != nil {
		return fmt.Errorf("historical uncovered batch: %w", err)
	}
	// Revisions beyond the head do not exist.
	st, body, err = query(eq, 75, "6")
	if err != nil {
		return err
	}
	return expectError(st, body, http.StatusNotFound, "REVISION_NOT_FOUND")
}

// scenarioIdempotency checks that retrying an operation replays its first
// result, while reusing the operation id with different parameters fails.
func scenarioIdempotency() error {
	eq := uniqueEq("EQ-IDEM")
	op := uniqueOp("i1")

	st, first, err := publish(eq, op, 0, 0, 50, `{"k":1}`)
	if err != nil {
		return err
	}
	if st != http.StatusCreated {
		return fmt.Errorf("first publish: status %d, want 201 (body %s)", st, first)
	}

	// Exact retry: same first result, no new revision.
	st, replay, err := publish(eq, op, 0, 0, 50, `{"k":1}`)
	if err != nil {
		return err
	}
	if st != http.StatusCreated {
		return fmt.Errorf("replay: status %d, want 201 (body %s)", st, replay)
	}
	if string(replay) != string(first) {
		return fmt.Errorf("replay body = %s, want first result %s", replay, first)
	}

	// Concurrent identical retries: every one of them replays the first
	// result; none may create a new revision or fail.
	const retries = 6
	type retryOutcome struct {
		status int
		body   []byte
		err    error
	}
	retryResults := make(chan retryOutcome, retries)
	var rwg sync.WaitGroup
	for i := 0; i < retries; i++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			st, body, err := publish(eq, op, 0, 0, 50, `{"k":1}`)
			retryResults <- retryOutcome{st, body, err}
		}()
	}
	rwg.Wait()
	close(retryResults)
	for o := range retryResults {
		if o.err != nil {
			return fmt.Errorf("concurrent retry failed: %w", o.err)
		}
		if o.status != http.StatusCreated || string(o.body) != string(first) {
			return fmt.Errorf("concurrent retry: status %d body %s, want 201 %s",
				o.status, o.body, first)
		}
	}

	// Same operation id with any parameter changed is a stable conflict.
	for _, tc := range []struct {
		name         string
		seen         int64
		lower, upper int64
		content      string
	}{
		{"different content", 0, 0, 50, `{"k":2}`},
		{"different range", 0, 0, 60, `{"k":1}`},
		{"different seen revision", 1, 0, 50, `{"k":1}`},
	} {
		st, body, err := publish(eq, op, tc.seen, tc.lower, tc.upper, tc.content)
		if err != nil {
			return err
		}
		if err := expectError(st, body, http.StatusConflict, "OPERATION_CONFLICT"); err != nil {
			return fmt.Errorf("%s: %w", tc.name, err)
		}
	}

	// Advance the head with a different operation...
	if err := expectPublish(eq, uniqueOp("i2"), 1, 50, 60, `"Z"`, 2,
		seg(0, 50, `{"k":1}`), seg(50, 60, `"Z"`)); err != nil {
		return err
	}
	// ...and the original operation still replays its first result.
	st, replay2, err := publish(eq, op, 0, 0, 50, `{"k":1}`)
	if err != nil {
		return err
	}
	if st != http.StatusCreated || string(replay2) != string(first) {
		return fmt.Errorf("replay after head moved: status %d body %s, want 201 %s", st, replay2, first)
	}
	// The head is still revision 2.
	return expectQuery(eq, 55, "", 2, 50, 60, `"Z"`)
}

// scenarioConcurrency races several publishes on the same seen revision:
// exactly one may win, the rest must fail with STALE_REVISION, and the head
// must advance by exactly one (no partial splits, no lost updates).
func scenarioConcurrency() error {
	eq := uniqueEq("EQ-CONC")
	if err := expectPublish(eq, uniqueOp("c0"), 0, 0, 1000, `"BASE"`, 1,
		seg(0, 1000, `"BASE"`)); err != nil {
		return err
	}

	const racers = 8
	type outcome struct {
		status int
		body   []byte
		err    error
	}
	results := make(chan outcome, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, body, err := publish(eq, uniqueOp(fmt.Sprintf("c%d", i+1)),
				1, int64(i*10), int64(i*10+10), fmt.Sprintf(`"V%d"`, i))
			results <- outcome{st, body, err}
		}(i)
	}
	wg.Wait()
	close(results)

	var created, stale int
	for o := range results {
		switch {
		case o.err != nil:
			return fmt.Errorf("racer request failed: %w", o.err)
		case o.status == http.StatusCreated:
			created++
		case o.status == http.StatusConflict &&
			strings.Contains(string(o.body), "STALE_REVISION"):
			stale++
		default:
			return fmt.Errorf("unexpected racer outcome: status %d body %s", o.status, o.body)
		}
	}
	if created != 1 || stale != racers-1 {
		return fmt.Errorf("created=%d stale=%d, want exactly 1 success and %d stale",
			created, stale, racers-1)
	}

	// Exactly one winner means the head is now revision 2; anything else
	// (lost update, double apply, partial split) breaks this publish.
	if err := expectPublish(eq, uniqueOp("c-final"), 2, 0, 1000, `"FINAL"`, 3,
		seg(0, 1000, `"FINAL"`)); err != nil {
		return fmt.Errorf("head did not advance by exactly one: %w", err)
	}
	return nil
}

// scenarioBatchPublish checks one indivisible package applied across several
// equipments: every equipment gets its own incremented revision and a full
// effective-interval snapshot, regardless of the wire order of the items.
func scenarioBatchPublish() error {
	eqA := uniqueEq("EQ-BA")
	eqB := uniqueEq("EQ-BB")
	eqC := uniqueEq("EQ-BC")

	// Give the equipments different histories so their next revisions differ.
	if err := expectPublish(eqA, uniqueOp("bp-a1"), 0, 0, 100, `"A"`, 1,
		seg(0, 100, `"A"`)); err != nil {
		return err
	}
	if err := expectPublish(eqA, uniqueOp("bp-a2"), 1, 100, 200, `"B"`, 2,
		seg(0, 100, `"A"`), seg(100, 200, `"B"`)); err != nil {
		return err
	}
	if err := expectPublish(eqB, uniqueOp("bp-b1"), 0, 0, 500, `"K"`, 1,
		seg(0, 500, `"K"`)); err != nil {
		return err
	}
	// eqC is fresh: it has no head row at all.

	// Items are deliberately not in equipment order on the wire.
	op := uniqueOp("bp")
	st, body, err := batchPublish(op,
		batchItem{eqB, 1, 100, 300, `"NEW"`},
		batchItem{eqC, 0, 10, 20, `"C0"`},
		batchItem{eqA, 2, 50, 150, `"X"`},
	)
	if err != nil {
		return err
	}
	if st != http.StatusCreated {
		return fmt.Errorf("batch publish: status %d, want 201 (body %s)", st, body)
	}
	var br batchResponse
	if err := json.Unmarshal(body, &br); err != nil {
		return fmt.Errorf("batch publish: response not JSON: %v (%s)", err, body)
	}
	if br.OperationID != op {
		return fmt.Errorf("batch publish: operation_id = %q, want %q", br.OperationID, op)
	}
	// Results come back in stable (ascending equipment) order, each with its
	// own next revision and complete snapshot.
	if len(br.Results) != 3 ||
		br.Results[0].Equipment != eqA || br.Results[1].Equipment != eqB || br.Results[2].Equipment != eqC {
		return fmt.Errorf("batch results not ordered by equipment: %s", mustJSON(br.Results))
	}
	if br.Results[0].Revision != 3 || br.Results[1].Revision != 2 || br.Results[2].Revision != 1 {
		return fmt.Errorf("batch revisions = %d,%d,%d, want 3,2,1",
			br.Results[0].Revision, br.Results[1].Revision, br.Results[2].Revision)
	}
	if err := expectSegments(br.Results[0].Segments, []segment{
		seg(0, 50, `"A"`), seg(50, 150, `"X"`), seg(150, 200, `"B"`)}); err != nil {
		return fmt.Errorf("eqA snapshot: %w", err)
	}
	if err := expectSegments(br.Results[1].Segments, []segment{
		seg(0, 100, `"K"`), seg(100, 300, `"NEW"`), seg(300, 500, `"K"`)}); err != nil {
		return fmt.Errorf("eqB snapshot: %w", err)
	}
	if err := expectSegments(br.Results[2].Segments, []segment{
		seg(10, 20, `"C0"`)}); err != nil {
		return fmt.Errorf("eqC snapshot: %w", err)
	}

	// The new heads are queryable on every equipment.
	if err := expectQuery(eqA, 75, "", 3, 50, 150, `"X"`); err != nil {
		return err
	}
	if err := expectQuery(eqB, 250, "", 2, 100, 300, `"NEW"`); err != nil {
		return err
	}
	return expectQuery(eqC, 15, "", 1, 10, 20, `"C0"`)
}

// scenarioBatchRollback proves the package is indivisible: when any item
// fails — stale seen revision, invalid interval, invalid content — no
// equipment in the package produces a new revision, and the error names the
// first failing equipment stably.
func scenarioBatchRollback() error {
	eqA := uniqueEq("EQ-RA")
	eqB := uniqueEq("EQ-RB")

	if err := expectPublish(eqA, uniqueOp("br-a1"), 0, 0, 100, `"A"`, 1,
		seg(0, 100, `"A"`)); err != nil {
		return err
	}
	if err := expectPublish(eqB, uniqueOp("br-b1"), 0, 0, 100, `"B"`, 1,
		seg(0, 100, `"B"`)); err != nil {
		return err
	}

	// eqB's seen revision is stale: the whole package must be rejected and
	// the error must locate eqB.
	st, body, err := batchPublish(uniqueOp("br-stale"),
		batchItem{eqA, 1, 0, 50, `"Z"`},
		batchItem{eqB, 7, 0, 50, `"Z"`},
	)
	if err != nil {
		return err
	}
	if err := expectItemError(st, body, http.StatusConflict, "STALE_REVISION", eqB); err != nil {
		return fmt.Errorf("stale item: %w", err)
	}

	// An invalid interval anywhere in the package rejects everything; the
	// failure is located at eqB even though eqA's item was fine.
	st, body, err = batchPublish(uniqueOp("br-interval"),
		batchItem{eqA, 1, 0, 50, `"Z"`},
		batchItem{eqB, 1, 60, 60, `"Z"`},
	)
	if err != nil {
		return err
	}
	if err := expectItemError(st, body, http.StatusBadRequest, "INVALID_INTERVAL", eqB); err != nil {
		return fmt.Errorf("invalid interval item: %w", err)
	}

	// Malformed content anywhere makes the whole body undecodable: the
	// package is rejected before any equipment is touched.
	st, body, err = batchPublishRaw(fmt.Sprintf(
		`{"operation_id":%q,"items":[`+
			`{"equipment":%q,"seen_revision":1,"lower":0,"upper":50,"content":"Z"},`+
			`{"equipment":%q,"seen_revision":1,"lower":0,"upper":50,"content":{bad json}}]}`,
		uniqueOp("br-content"), eqA, eqB))
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusBadRequest, "INVALID_JSON"); err != nil {
		return fmt.Errorf("malformed content item: %w", err)
	}

	// Duplicate equipment in one package is rejected and located.
	st, body, err = batchPublish(uniqueOp("br-dup"),
		batchItem{eqA, 1, 0, 50, `"Z"`},
		batchItem{eqA, 1, 50, 60, `"Z"`},
	)
	if err != nil {
		return err
	}
	if err := expectItemError(st, body, http.StatusBadRequest, "INVALID_PARAMETER", eqA); err != nil {
		return fmt.Errorf("duplicate equipment: %w", err)
	}

	// Crucially: none of the failures above advanced any head. Both
	// equipments must still be at revision 1 with their original content,
	// and publishing with seen_revision 1 must still succeed.
	if err := expectQuery(eqA, 10, "", 1, 0, 100, `"A"`); err != nil {
		return fmt.Errorf("eqA moved despite package rollback: %w", err)
	}
	if err := expectQuery(eqB, 10, "", 1, 0, 100, `"B"`); err != nil {
		return fmt.Errorf("eqB moved despite package rollback: %w", err)
	}
	if err := expectPublish(eqA, uniqueOp("br-a2"), 1, 0, 50, `"OK"`, 2,
		seg(0, 50, `"OK"`), seg(50, 100, `"A"`)); err != nil {
		return fmt.Errorf("eqA head not at 1 after rollbacks: %w", err)
	}
	return expectPublish(eqB, uniqueOp("br-b2"), 1, 0, 50, `"OK"`, 2,
		seg(0, 50, `"OK"`), seg(50, 100, `"B"`))
}

// scenarioBatchIdempotency checks that the same global operation id with the
// byte-identical package replays the first complete result (even under
// concurrency and in a different item order), while any parameter change is
// rejected — and that the id shares the single-publish namespace.
func scenarioBatchIdempotency() error {
	eqA := uniqueEq("EQ-IA")
	eqB := uniqueEq("EQ-IB")
	op := uniqueOp("bi")

	items := []batchItem{
		{eqA, 0, 0, 100, `{"k":1}`},
		{eqB, 0, 0, 200, `{"k":2}`},
	}
	st, first, err := batchPublish(op, items...)
	if err != nil {
		return err
	}
	if st != http.StatusCreated {
		return fmt.Errorf("first batch publish: status %d, want 201 (body %s)", st, first)
	}

	// Exact retry, items in the opposite wire order: same first result.
	st, replay, err := batchPublish(op, items[1], items[0])
	if err != nil {
		return err
	}
	if st != http.StatusCreated {
		return fmt.Errorf("batch replay: status %d, want 201 (body %s)", st, replay)
	}
	if string(replay) != string(first) {
		return fmt.Errorf("batch replay body = %s, want first result %s", replay, first)
	}

	// Concurrent identical retries: all replay the first result.
	const retries = 6
	type retryOutcome struct {
		status int
		body   []byte
		err    error
	}
	retryResults := make(chan retryOutcome, retries)
	var rwg sync.WaitGroup
	for i := 0; i < retries; i++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			st, body, err := batchPublish(op, items...)
			retryResults <- retryOutcome{st, body, err}
		}()
	}
	rwg.Wait()
	close(retryResults)
	for o := range retryResults {
		if o.err != nil {
			return fmt.Errorf("concurrent batch retry failed: %w", o.err)
		}
		if o.status != http.StatusCreated || string(o.body) != string(first) {
			return fmt.Errorf("concurrent batch retry: status %d body %s, want 201 %s",
				o.status, o.body, first)
		}
	}

	// Same operation id with any parameter changed is a stable conflict.
	for _, tc := range []struct {
		name  string
		items []batchItem
	}{
		{"different content", []batchItem{{eqA, 0, 0, 100, `{"k":9}`}, {eqB, 0, 0, 200, `{"k":2}`}}},
		{"different range", []batchItem{{eqA, 0, 0, 101, `{"k":1}`}, {eqB, 0, 0, 200, `{"k":2}`}}},
		{"different seen revision", []batchItem{{eqA, 1, 0, 100, `{"k":1}`}, {eqB, 0, 0, 200, `{"k":2}`}}},
		{"missing item", []batchItem{{eqA, 0, 0, 100, `{"k":1}`}}},
		{"extra item", []batchItem{{eqA, 0, 0, 100, `{"k":1}`}, {eqB, 0, 0, 200, `{"k":2}`}, {uniqueEq("EQ-IX"), 0, 0, 1, `null`}}},
	} {
		st, body, err := batchPublish(op, tc.items...)
		if err != nil {
			return err
		}
		if err := expectError(st, body, http.StatusConflict, "OPERATION_CONFLICT"); err != nil {
			return fmt.Errorf("%s: %w", tc.name, err)
		}
	}

	// The global operation id is also bound per equipment: the single
	// endpoint replays the same operation for a package member...
	st, singleReplay, err := publish(eqA, op, 0, 0, 100, `{"k":1}`)
	if err != nil {
		return err
	}
	if st != http.StatusCreated {
		return fmt.Errorf("single-endpoint replay of batch op: status %d, want 201 (body %s)",
			st, singleReplay)
	}
	var pr publishResponse
	if err := json.Unmarshal(singleReplay, &pr); err != nil {
		return fmt.Errorf("single-endpoint replay: response not JSON: %v (%s)", err, singleReplay)
	}
	if pr.Revision != 1 {
		return fmt.Errorf("single-endpoint replay: revision = %d, want 1", pr.Revision)
	}
	// ...and rejects it with different parameters.
	st, body, err := publish(eqA, op, 0, 0, 100, `{"k":2}`)
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusConflict, "OPERATION_CONFLICT"); err != nil {
		return fmt.Errorf("single-endpoint conflict with batch op: %w", err)
	}

	// No retry or conflict created revisions: both heads are still 1.
	if err := expectQuery(eqA, 50, "", 1, 0, 100, `{"k":1}`); err != nil {
		return err
	}
	return expectQuery(eqB, 100, "", 1, 0, 200, `{"k":2}`)
}

// scenarioBatchInterleave races batch packages against single publishes on
// overlapping equipments. Competing in one global order means: no deadlock,
// every outcome is either a full success or a clean conflict, heads advance
// exactly per committed package, and historical queries are unaffected.
func scenarioBatchInterleave() error {
	eqA := uniqueEq("EQ-XA")
	eqB := uniqueEq("EQ-XB")
	if err := expectPublish(eqA, uniqueOp("bx-a1"), 0, 0, 1000, `"A0"`, 1,
		seg(0, 1000, `"A0"`)); err != nil {
		return err
	}
	if err := expectPublish(eqB, uniqueOp("bx-b1"), 0, 0, 1000, `"B0"`, 1,
		seg(0, 1000, `"B0"`)); err != nil {
		return err
	}

	// Two batches (overlapping on eqA+eqB and eqB alone) race with single
	// publishes on both equipments, all seeing revision 1. The lock order is
	// global, so this cannot deadlock; each racer either commits its whole
	// package or loses cleanly.
	type outcome struct {
		kind   string
		status int
		body   []byte
		err    error
	}
	results := make(chan outcome, 4)
	var wg sync.WaitGroup
	racers := []func() (int, []byte, error){
		func() (int, []byte, error) {
			return batchPublish(uniqueOp("bx-batch1"),
				batchItem{eqA, 1, 0, 500, `"BA"`},
				batchItem{eqB, 1, 0, 500, `"BB"`})
		},
		func() (int, []byte, error) {
			return batchPublish(uniqueOp("bx-batch2"),
				batchItem{eqB, 1, 500, 700, `"B2"`})
		},
		func() (int, []byte, error) {
			return publish(eqA, uniqueOp("bx-single-a"), 1, 500, 900, `"SA"`)
		},
		func() (int, []byte, error) {
			return publish(eqB, uniqueOp("bx-single-b"), 1, 700, 900, `"SB"`)
		},
	}
	kinds := []string{"batch1", "batch2", "single-a", "single-b"}
	for i, racer := range racers {
		wg.Add(1)
		go func(kind string, fn func() (int, []byte, error)) {
			defer wg.Done()
			st, body, err := fn()
			results <- outcome{kind, st, body, err}
		}(kinds[i], racer)
	}
	wg.Wait()
	close(results)

	won := map[string]bool{}
	for o := range results {
		switch {
		case o.err != nil:
			return fmt.Errorf("racer %s failed: %w", o.kind, o.err)
		case o.status == http.StatusCreated:
			won[o.kind] = true
		case o.status == http.StatusConflict &&
			strings.Contains(string(o.body), "STALE_REVISION"):
			won[o.kind] = false
		default:
			return fmt.Errorf("racer %s: unexpected outcome status %d body %s",
				o.kind, o.status, o.body)
		}
	}

	// Exactly one writer per equipment could win revision 1 -> 2. batch1
	// claims both equipments, so if it won, nothing else could have.
	if won["batch1"] && (won["batch2"] || won["single-a"] || won["single-b"]) {
		return fmt.Errorf("batch1 committed but another racer also won: %v", won)
	}
	// batch2 and single-b both claim eqB alone: at most one of them.
	if won["batch2"] && won["single-b"] {
		return fmt.Errorf("batch2 and single-b both won eqB: %v", won)
	}
	winsA, winsB := 0, 0
	if won["batch1"] {
		winsA, winsB = 1, 1
	}
	if won["single-a"] {
		winsA++
	}
	if won["batch2"] || won["single-b"] {
		winsB++
	}
	if winsA != 1 || winsB != 1 {
		return fmt.Errorf("winners per equipment = %d,%d, want exactly 1,1 (%v)", winsA, winsB, won)
	}

	// Each equipment's head advanced by exactly one committed package: a
	// follow-up publish at seen_revision 2 must succeed with revision 3 on
	// both equipments (anything else — lost update, double apply, partial
	// package — breaks this).
	if err := expectPublishAt(eqA, uniqueOp("bx-a-next"), 2, 0, 10, `"F"`, 3); err != nil {
		return fmt.Errorf("eqA head did not advance by exactly one: %w", err)
	}
	if err := expectPublishAt(eqB, uniqueOp("bx-b-next"), 2, 0, 10, `"F"`, 3); err != nil {
		return fmt.Errorf("eqB head did not advance by exactly one: %w", err)
	}

	// History queries after the interleaved commits: revision 1 on both
	// equipments is exactly what it was before the race.
	if err := expectQuery(eqA, 750, "1", 1, 0, 1000, `"A0"`); err != nil {
		return err
	}
	if err := expectQuery(eqB, 750, "1", 1, 0, 1000, `"B0"`); err != nil {
		return err
	}
	// Revision 2 reflects exactly the winning package on each equipment.
	if won["batch1"] {
		if err := expectQuery(eqA, 250, "2", 2, 0, 500, `"BA"`); err != nil {
			return err
		}
		return expectQuery(eqB, 250, "2", 2, 0, 500, `"BB"`)
	}
	if err := expectQuery(eqA, 750, "2", 2, 500, 900, `"SA"`); err != nil {
		return err
	}
	if won["batch2"] {
		return expectQuery(eqB, 600, "2", 2, 500, 700, `"B2"`)
	}
	return expectQuery(eqB, 800, "2", 2, 700, 900, `"SB"`)
}

// expectPublishAt publishes and checks only the resulting revision, leaving
// the snapshot unchecked.
func expectPublishAt(eq, op string, seen, lower, upper int64, content string, wantRev int64) error {
	st, body, err := publish(eq, op, seen, lower, upper, content)
	if err != nil {
		return err
	}
	if st != http.StatusCreated {
		return fmt.Errorf("publish %s: status %d, want 201 (body %s)", op, st, body)
	}
	var pr publishResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return fmt.Errorf("publish %s: response not JSON: %v (%s)", op, err, body)
	}
	if pr.Revision != wantRev {
		return fmt.Errorf("publish %s: revision = %d, want %d", op, pr.Revision, wantRev)
	}
	return nil
}

// expectItemError checks a batch item failure: stable code plus the equipment
// identifier of the first failing item.
func expectItemError(st int, body []byte, wantStatus int, wantCode, wantEquipment string) error {
	if st != wantStatus {
		return fmt.Errorf("status %d, want %d (body %s)", st, wantStatus, body)
	}
	var er struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			Equipment string `json:"equipment"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &er); err != nil {
		return fmt.Errorf("error body not JSON: %v (%s)", err, body)
	}
	if er.Error.Code != wantCode {
		return fmt.Errorf("error code = %q, want %q (body %s)", er.Error.Code, wantCode, body)
	}
	if er.Error.Equipment != wantEquipment {
		return fmt.Errorf("error equipment = %q, want %q (body %s)",
			er.Error.Equipment, wantEquipment, body)
	}
	return nil
}

// scenarioValidation checks input validation and stable error codes, and
// that failed transactions leave no trace behind.
func scenarioValidation() error {
	eq := uniqueEq("EQ-ERR")

	// Invalid intervals.
	for _, tc := range [][2]int64{{50, 50}, {60, 50}} {
		st, body, err := publish(eq, uniqueOp("e-interval"), 0, tc[0], tc[1], `"X"`)
		if err != nil {
			return err
		}
		if err := expectError(st, body, http.StatusBadRequest, "INVALID_INTERVAL"); err != nil {
			return fmt.Errorf("interval [%d,%d): %w", tc[0], tc[1], err)
		}
	}

	// Stale revision on a fresh equipment.
	st, body, err := publish(eq, uniqueOp("e-stale"), 3, 0, 10, `"X"`)
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusConflict, "STALE_REVISION"); err != nil {
		return fmt.Errorf("stale publish: %w", err)
	}

	// Missing or malformed parameters.
	rawCases := []struct {
		name string
		body string
		code string
	}{
		{"missing operation_id", `{"seen_revision":0,"lower":0,"upper":10,"content":"X"}`, "INVALID_PARAMETER"},
		{"missing seen_revision", `{"operation_id":"x","lower":0,"upper":10,"content":"X"}`, "INVALID_PARAMETER"},
		{"missing lower", `{"operation_id":"x","seen_revision":0,"upper":10,"content":"X"}`, "INVALID_PARAMETER"},
		{"missing upper", `{"operation_id":"x","seen_revision":0,"lower":0,"content":"X"}`, "INVALID_PARAMETER"},
		{"missing content", `{"operation_id":"x","seen_revision":0,"lower":0,"upper":10}`, "INVALID_PARAMETER"},
		{"negative seen_revision", `{"operation_id":"x","seen_revision":-1,"lower":0,"upper":10,"content":"X"}`, "INVALID_PARAMETER"},
		{"malformed JSON", `{not json`, "INVALID_JSON"},
		{"unknown field", `{"operation_id":"x","seen_revision":0,"lower":0,"upper":10,"content":"X","bogus":1}`, "INVALID_JSON"},
		{"invalid content", `{"operation_id":"x","seen_revision":0,"lower":0,"upper":10,"content":}`, "INVALID_JSON"},
	}
	for _, tc := range rawCases {
		st, body, err := publishRaw(eq, tc.body)
		if err != nil {
			return err
		}
		if err := expectError(st, body, http.StatusBadRequest, tc.code); err != nil {
			return fmt.Errorf("%s: %w", tc.name, err)
		}
	}

	// Every publish above failed inside its transaction: the equipment must
	// not exist at all (no partial head row, no partial split).
	st, body, err = query(eq, 5, "")
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusNotFound, "EQUIPMENT_NOT_FOUND"); err != nil {
		return fmt.Errorf("failed transactions left state behind: %w", err)
	}

	// Query parameter validation.
	st, body, err = doJSON(http.MethodGet,
		fmt.Sprintf("%s/v1/equipments/%s/calibration", appURL, url.PathEscape(splitEq)), "")
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusBadRequest, "INVALID_PARAMETER"); err != nil {
		return fmt.Errorf("missing batch: %w", err)
	}
	st, body, err = query(splitEq, 10, "abc")
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusBadRequest, "INVALID_PARAMETER"); err != nil {
		return fmt.Errorf("non-integer revision: %w", err)
	}
	st, body, err = query(splitEq, 10, "0")
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusBadRequest, "INVALID_PARAMETER"); err != nil {
		return fmt.Errorf("revision below 1: %w", err)
	}
	st, body, err = query(splitEq, 10, "999")
	if err != nil {
		return err
	}
	if err := expectError(st, body, http.StatusNotFound, "REVISION_NOT_FOUND"); err != nil {
		return fmt.Errorf("future revision: %w", err)
	}

	// Health endpoint smoke.
	st, body, err = doJSON(http.MethodGet, appURL+"/health", "")
	if err != nil {
		return err
	}
	if st != http.StatusOK || !strings.Contains(string(body), `"status":"ok"`) {
		return fmt.Errorf("health: status %d body %s, want 200 {\"status\":\"ok\"}", st, body)
	}
	return nil
}

// scenarioDBImmutability connects to PostgreSQL directly and proves that
// historical rows cannot be rewritten or deleted.
func scenarioDBImmutability() error {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx,
		`UPDATE segments SET content = '"HACKED"' WHERE equipment = $1`, splitEq); err == nil {
		return fmt.Errorf("UPDATE on historical segments succeeded, want immutability error")
	} else if !strings.Contains(err.Error(), "immutable") {
		return fmt.Errorf("UPDATE rejected with unexpected error: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM segments WHERE equipment = $1`, splitEq); err == nil {
		return fmt.Errorf("DELETE on historical segments succeeded, want immutability error")
	} else if !strings.Contains(err.Error(), "immutable") {
		return fmt.Errorf("DELETE rejected with unexpected error: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE operations SET response = '{}' WHERE equipment = $1`, splitEq); err == nil {
		return fmt.Errorf("UPDATE on operation ledger succeeded, want immutability error")
	}
	return nil
}
