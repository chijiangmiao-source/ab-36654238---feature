package split

import (
	"reflect"
	"testing"
)

func seg(l, u int64, c string) Segment { return Segment{Lower: l, Upper: u, Content: c} }

func TestApply(t *testing.T) {
	tests := []struct {
		name    string
		segs    []Segment
		lower   int64
		upper   int64
		content string
		want    []Segment
	}{
		{
			name:  "empty state",
			segs:  nil,
			lower: 10, upper: 20, content: "A",
			want: []Segment{seg(10, 20, "A")},
		},
		{
			name:  "disjoint after existing keeps gap",
			segs:  []Segment{seg(0, 5, "X")},
			lower: 10, upper: 20, content: "A",
			want: []Segment{seg(0, 5, "X"), seg(10, 20, "A")},
		},
		{
			name:  "disjoint before existing keeps gap",
			segs:  []Segment{seg(10, 20, "X")},
			lower: 0, upper: 5, content: "A",
			want: []Segment{seg(0, 5, "A"), seg(10, 20, "X")},
		},
		{
			name:  "exact cover replaces",
			segs:  []Segment{seg(0, 10, "X")},
			lower: 0, upper: 10, content: "A",
			want: []Segment{seg(0, 10, "A")},
		},
		{
			name:  "punch middle keeps both residuals",
			segs:  []Segment{seg(0, 100, "X")},
			lower: 40, upper: 60, content: "A",
			want: []Segment{seg(0, 40, "X"), seg(40, 60, "A"), seg(60, 100, "X")},
		},
		{
			name:  "overlap right half keeps left residual",
			segs:  []Segment{seg(0, 50, "X")},
			lower: 30, upper: 80, content: "A",
			want: []Segment{seg(0, 30, "X"), seg(30, 80, "A")},
		},
		{
			name:  "overlap left half keeps right residual",
			segs:  []Segment{seg(50, 100, "X")},
			lower: 20, upper: 60, content: "A",
			want: []Segment{seg(20, 60, "A"), seg(60, 100, "X")},
		},
		{
			name:  "span multiple segments keeps outer residuals",
			segs:  []Segment{seg(0, 10, "A"), seg(10, 20, "B"), seg(20, 30, "C")},
			lower: 5, upper: 25, content: "X",
			want: []Segment{seg(0, 5, "A"), seg(5, 25, "X"), seg(25, 30, "C")},
		},
		{
			name:  "cover several segments entirely",
			segs:  []Segment{seg(0, 10, "A"), seg(10, 20, "B"), seg(20, 30, "C")},
			lower: 10, upper: 30, content: "X",
			want: []Segment{seg(0, 10, "A"), seg(10, 30, "X")},
		},
		{
			name:  "adjacent right with same content merges",
			segs:  []Segment{seg(0, 10, "A")},
			lower: 10, upper: 20, content: "A",
			want: []Segment{seg(0, 20, "A")},
		},
		{
			name:  "adjacent left with same content merges",
			segs:  []Segment{seg(10, 20, "A")},
			lower: 0, upper: 10, content: "A",
			want: []Segment{seg(0, 20, "A")},
		},
		{
			name:  "bridges two equal segments into one",
			segs:  []Segment{seg(0, 10, "A"), seg(20, 30, "A")},
			lower: 10, upper: 20, content: "A",
			want: []Segment{seg(0, 30, "A")},
		},
		{
			name:  "republish identical content is identity",
			segs:  []Segment{seg(0, 10, "A")},
			lower: 0, upper: 10, content: "A",
			want: []Segment{seg(0, 10, "A")},
		},
		{
			name:  "left residual merges with new segment of same content",
			segs:  []Segment{seg(0, 10, "A"), seg(10, 20, "B")},
			lower: 5, upper: 15, content: "A",
			want: []Segment{seg(0, 15, "A"), seg(15, 20, "B")},
		},
		{
			name:  "right residual merges with new segment of same content",
			segs:  []Segment{seg(0, 10, "B"), seg(10, 20, "A")},
			lower: 5, upper: 15, content: "A",
			want: []Segment{seg(0, 5, "B"), seg(5, 20, "A")},
		},
		{
			name:  "gap between segments is preserved",
			segs:  []Segment{seg(0, 10, "A"), seg(20, 30, "B")},
			lower: 30, upper: 40, content: "C",
			want: []Segment{seg(0, 10, "A"), seg(20, 30, "B"), seg(30, 40, "C")},
		},
		{
			name:  "publish inside a gap does not merge across it",
			segs:  []Segment{seg(0, 10, "A"), seg(20, 30, "A")},
			lower: 12, upper: 15, content: "A",
			want: []Segment{seg(0, 10, "A"), seg(12, 15, "A"), seg(20, 30, "A")},
		},
		{
			name:  "negative batch numbers",
			segs:  []Segment{seg(-100, 0, "X")},
			lower: -50, upper: 50, content: "A",
			want: []Segment{seg(-100, -50, "X"), seg(-50, 50, "A")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Apply(tt.segs, tt.lower, tt.upper, tt.content)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Apply() = %+v, want %+v", got, tt.want)
			}
			assertInvariants(t, got)
		})
	}
}

// assertInvariants verifies the result is sorted, non-overlapping,
// non-degenerate and free of mergeable adjacent segments.
func assertInvariants(t *testing.T, segs []Segment) {
	t.Helper()
	for i, s := range segs {
		if s.Lower >= s.Upper {
			t.Fatalf("degenerate segment %+v", s)
		}
		if i > 0 {
			prev := segs[i-1]
			if prev.Upper > s.Lower {
				t.Fatalf("segments overlap: %+v and %+v", prev, s)
			}
			if prev.Upper == s.Lower && prev.Content == s.Content {
				t.Fatalf("adjacent equal-content segments not merged: %+v and %+v", prev, s)
			}
		}
	}
}
