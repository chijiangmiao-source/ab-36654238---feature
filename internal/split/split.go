// Package split implements the interval arithmetic behind calibration
// segments: how a newly published half-open batch interval replaces the
// overlapping part of the previous revision while preserving residuals and
// merging adjacent segments that carry identical content.
package split

// Segment is a half-open batch interval [Lower, Upper) carrying a calibration
// content payload (canonical JSON). A sorted, non-overlapping list of
// segments describes the full calibration state of one equipment revision;
// batch numbers not covered by any segment are "uncovered".
type Segment struct {
	Lower   int64
	Upper   int64
	Content string
}

// Apply returns the segment list that results from publishing the half-open
// interval [lower, upper) with the given content on top of segs.
//
// The overlap between [lower, upper) and any existing segment is replaced by
// the new segment; overlapped segments leave their non-overlapping left/right
// parts behind as residual segments; disjoint segments are kept untouched.
// Afterwards, adjacent segments with identical content are merged.
//
// segs must be sorted by Lower, non-overlapping and non-degenerate; the
// result keeps the same invariants. lower must be strictly less than upper.
func Apply(segs []Segment, lower, upper int64, content string) []Segment {
	out := make([]Segment, 0, len(segs)+2)
	inserted := false
	for _, s := range segs {
		switch {
		case s.Upper <= lower:
			// Fully before the published interval: untouched.
			out = append(out, s)
		case s.Lower >= upper:
			// Fully after the published interval: untouched.
			if !inserted {
				out = append(out, Segment{Lower: lower, Upper: upper, Content: content})
				inserted = true
			}
			out = append(out, s)
		default:
			// Overlapping: keep the left residual, replace the overlap with
			// the new segment, keep the right residual.
			if s.Lower < lower {
				out = append(out, Segment{Lower: s.Lower, Upper: lower, Content: s.Content})
			}
			if !inserted {
				out = append(out, Segment{Lower: lower, Upper: upper, Content: content})
				inserted = true
			}
			if s.Upper > upper {
				out = append(out, Segment{Lower: upper, Upper: s.Upper, Content: s.Content})
			}
		}
	}
	if !inserted {
		out = append(out, Segment{Lower: lower, Upper: upper, Content: content})
	}
	return merge(out)
}

// merge coalesces adjacent segments ([a, b) and [b, c)) that carry identical
// content. segs must be sorted and non-overlapping.
func merge(segs []Segment) []Segment {
	if len(segs) == 0 {
		return segs
	}
	out := segs[:1]
	for _, s := range segs[1:] {
		last := &out[len(out)-1]
		if last.Upper == s.Lower && last.Content == s.Content {
			last.Upper = s.Upper
			continue
		}
		out = append(out, s)
	}
	return out
}
