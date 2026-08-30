package store

import (
	"context"
	"encoding/json"
	"testing"
)

func BenchmarkClaim(b *testing.B) {
	dsn := "postgres://hopper:hopper@localhost/hopper?sslmode=disable"
	s, err := Open(dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		payload, _ := json.Marshal(map[string]int{"i": i})
		s.Enqueue(ctx, "bench", "bench_job", payload)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Claim(ctx, "bench-worker")
	}
}

func BenchmarkClaimSingleQuery(b *testing.B) {
	dsn := "postgres://hopper:hopper@localhost/hopper?sslmode=disable"
	s, err := Open(dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		payload, _ := json.Marshal(map[string]int{"i": i})
		s.Enqueue(ctx, "bench", "bench_job", payload)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ClaimSingleQuery(ctx, "bench-worker")
	}
}
