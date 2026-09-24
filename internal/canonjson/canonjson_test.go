package canonjson

import "testing"

func TestNormalize(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "object keys sorted", in: `{ "b": 2, "a": 1 }`, want: `{"a":1,"b":2}`},
		{name: "nested objects sorted", in: `{"z":{"y":1,"x":2},"a":[]}`, want: `{"a":[],"z":{"x":2,"y":1}}`},
		{name: "string", in: `"A"`, want: `"A"`},
		{name: "array keeps order", in: `[ 3, 1, 2 ]`, want: `[3,1,2]`},
		{name: "number literal preserved", in: `{"v": 1.50}`, want: `{"v":1.50}`},
		{name: "whitespace only is error", in: "  \n ", wantErr: true},
		{name: "empty is error", in: "", wantErr: true},
		{name: "malformed is error", in: `{bad`, wantErr: true},
		{name: "trailing garbage is error", in: `{"a":1} {"b":2}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Normalize([]byte(tt.in))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Normalize(%q) = %q, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Normalize(%q) error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("Normalize(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalizeEqualityForMerging(t *testing.T) {
	a, err := Normalize([]byte(`{"p": 1, "q": [1, 2]}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Normalize([]byte(`{"q":[1,2],"p":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("canonical forms differ: %q vs %q", a, b)
	}
}
