// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package nomad

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/policy"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/hashicorp/nomad/api"
)

// countGoroutinesMatching returns the number of goroutines whose stack contains
// the given substring. This is robust against unrelated goroutines (e.g. the
// httptest server) that a raw runtime.NumGoroutine() count would include.
func countGoroutinesMatching(sub string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, sub) {
			count++
		}
	}
	return count
}

// waitForMatch polls until countGoroutinesMatching(sub) == want or the deadline
// passes, returning the last observed count.
func waitForMatch(sub string, want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	var got int
	for {
		got = countGoroutinesMatching(sub)
		if got == want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSource_MonitorPolicy_leaksBlockingQueryOnCancel reproduces the goroutine
// leak observed in prod: MonitorPolicy runs its Nomad blocking query in a
// child goroutine but only selects on ctx/reload in the parent. When the
// context is cancelled (which is what the policy manager does every time a
// policy is removed), MonitorPolicy returns immediately and abandons the
// in-flight blocking-query goroutine, which stays parked in the Nomad API call.
//
// Under Koyeb's constant policy churn this leaks one goroutine per removal,
// each pinning its heap, growing the process until it OOMs.
//
// The fix is to plumb the context into the query
// (q = q.WithContext(ctx)) so cancelling ctx aborts the request and the child
// goroutine returns. With that fix this test passes; without it, it fails.
func TestSource_MonitorPolicy_leaksBlockingQueryOnCancel(t *testing.T) {
	// The marker is the func literal inside MonitorPolicy that performs the
	// blocking GetPolicy call.
	const marker = "policy/nomad.(*Source).MonitorPolicy.func1"

	// Mock Nomad whose scaling-policy endpoint blocks like a real blocking
	// long-poll. It only unblocks when the client cancels the HTTP request
	// (r.Context().Done()) or when the test tears down.
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	src := TestNomadSource(t, func(c *api.Config, _ *policy.ConfigDefaults) {
		c.Address = srv.URL
	})

	req := policy.MonitorPolicyReq{
		ID:       "11111111-1111-1111-1111-111111111111",
		ErrCh:    make(chan error),
		ReloadCh: make(chan struct{}),
		ResultCh: make(chan sdk.ScalingPolicy),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		src.MonitorPolicy(ctx, req)
		close(done)
	}()

	// Wait until the blocking query is actually in flight.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking query never started")
	}

	// Positive control: the blocking-query goroutine must exist right now.
	if got := waitForMatch(marker, 1, 2*time.Second); got != 1 {
		t.Fatalf("expected 1 in-flight blocking-query goroutine, got %d", got)
	}

	// Simulate the manager removing the policy: cancel the handler context.
	cancel()

	// MonitorPolicy must return promptly on cancel.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("MonitorPolicy did not return after ctx cancel")
	}

	// A correct implementation cancels the in-flight query too, so the
	// blocking-query goroutine unwinds. The bug leaves it parked forever.
	if leaked := waitForMatch(marker, 0, 2*time.Second); leaked != 0 {
		t.Fatalf("goroutine leak: %d blocking-query goroutine(s) still parked after ctx cancel; "+
			"MonitorPolicy abandoned its in-flight Nomad query", leaked)
	}
}
