package main

import (
	"reflect"
	"testing"
)

func TestReadOrderLeaderModePreservesWriteRetryOrder(t *testing.T) {
	target := routeTarget{
		ShardID: 7,
		Leader:  "n2",
		Voters:  []string{"n1", "n2", "n3"},
	}

	got := readOrder(target, []string{"n4"}, "leader", 42)
	want := []string{"n2", "n1", "n3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("read order = %v, want %v", got, want)
	}
}

func TestReadOrderVotersModeRotatesAcrossVoters(t *testing.T) {
	target := routeTarget{
		ShardID: 7,
		Leader:  "n2",
		Voters:  []string{"n1", "n2", "n3"},
	}

	cases := []struct {
		seed int64
		want []string
	}{
		{seed: 0, want: []string{"n1", "n2", "n3"}},
		{seed: 1, want: []string{"n2", "n3", "n1"}},
		{seed: 2, want: []string{"n3", "n1", "n2"}},
		{seed: 4, want: []string{"n2", "n3", "n1"}},
	}
	for _, tc := range cases {
		got := readOrder(target, nil, "voters", tc.seed)
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("seed %d read order = %v, want %v", tc.seed, got, tc.want)
		}
	}
}

func TestReadOrderVotersModeDeduplicatesAndFallsBack(t *testing.T) {
	target := routeTarget{
		ShardID: 7,
		Leader:  "n2",
		Voters:  []string{"n1", "n2", "n2", "n3"},
	}
	got := readOrder(target, nil, "voters", 1)
	want := []string{"n2", "n3", "n1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("read order = %v, want %v", got, want)
	}

	got = readOrder(routeTarget{ShardID: 8}, []string{"n4", "n5"}, "voters", 0)
	want = []string{"n4", "n5"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback read order = %v, want %v", got, want)
	}
}
