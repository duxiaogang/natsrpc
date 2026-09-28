package gogopb

import (
	"testing"

	"github.com/gogo/protobuf/types"
)

func TestEncoderRejectsNonProtoValues(t *testing.T) {
	for _, value := range []interface{}{nil, "not a message", &struct{}{}} {
		if _, err := (Encoder{}).Encode(value); err == nil {
			t.Errorf("Encode(%T) should return an error", value)
		}
		if err := (Encoder{}).Decode(nil, value); err == nil {
			t.Errorf("Decode into %T should return an error", value)
		}
	}
}

func TestEncoderRoundTrip(t *testing.T) {
	encoder := Encoder{}
	want := &types.StringValue{Value: "hello"}
	payload, err := encoder.Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got := &types.StringValue{}
	if err := encoder.Decode(payload, got); err != nil {
		t.Fatal(err)
	}
	if got.Value != want.Value {
		t.Fatalf("decoded value = %q, want %q", got.Value, want.Value)
	}
}
