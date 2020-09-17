package arangodb

import (
	"reflect"
	"testing"

	"github.com/sbezverk/gobmp/pkg/message"
)

func TestFIFO(t *testing.T) {
	tests := []struct {
		name   string
		total  int
		index  int
		expect *message.UnicastPrefix
	}{
		{
			name:   "empty stack",
			total:  0,
			index:  0,
			expect: nil,
		},
		{
			name:  "1 element",
			total: 1,
			index: 1,
			expect: &message.UnicastPrefix{
				Sequence: 1,
			},
		},
		{
			name:  "2 elements",
			total: 2,
			index: 1,
			expect: &message.UnicastPrefix{
				Sequence: 1,
			},
		},
		{
			name:  "2 elements",
			total: 2,
			index: 2,
			expect: &message.UnicastPrefix{
				Sequence: 2,
			},
		},
		{
			name:  "3 elements",
			total: 3,
			index: 2,
			expect: &message.UnicastPrefix{
				Sequence: 2,
			},
		},
		{
			name:  "3 elements",
			total: 3,
			index: 3,
			expect: &message.UnicastPrefix{
				Sequence: 3,
			},
		},
		{
			name:  "100 elements",
			total: 100,
			index: 50,
			expect: &message.UnicastPrefix{
				Sequence: 50,
			},
		},
	}
	for _, tt := range tests {
		ff := newUnicastPrefixFIFO()
		for i := 0; i < tt.total; i++ {
			ff.Push(&message.UnicastPrefix{
				Sequence: i + 1,
			})
		}
		var result *message.UnicastPrefix
		for i := 0; i < tt.index; i++ {
			result = ff.Pop()
		}
		if !reflect.DeepEqual(tt.expect, result) {
			t.Fatalf("expected %+v and actual %+v do not match", tt.expect, result)
		}
	}
}
