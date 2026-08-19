package redis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

func newVersionGate(t *testing.T) (*PolicyVersionGate, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewPolicyVersionGate(rdb, DefaultPolicyVersionAttribute, DefaultPolicyVersionKey, testTenantAttribute), s
}

const testTenantAttribute = "userid"

// newTenantVersionGate builds a gate whose version key is namespaced per tenant.
func newTenantVersionGate(t *testing.T) (*PolicyVersionGate, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewPolicyVersionGate(rdb, DefaultPolicyVersionAttribute, DefaultPolicyVersionKey+":"+TenantPlaceholder, testTenantAttribute), s
}

func applyVersion(t *testing.T, gate *PolicyVersionGate, want string) (pipeline.Verdict, error) {
	t.Helper()
	md := map[string]string{}
	if want != "" {
		md[DefaultPolicyVersionAttribute] = want
	}
	msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req1", Metadata: md})
	var releases []pipeline.GateReleaseFunc
	return gate.Apply(context.Background(), msg, &releases)
}

func TestPolicyVersionGate_Verdicts(t *testing.T) {
	tests := []struct {
		name     string
		live     string // "" means the key is left unset
		want     string // "" means the attribute is absent
		expected pipeline.VerdictAction
	}{
		{name: "attribute absent", live: "7", want: "", expected: pipeline.ActionContinue},
		{name: "no live version published", live: "", want: "7", expected: pipeline.ActionContinue},
		{name: "versions match", live: "7", want: "7", expected: pipeline.ActionContinue},
		{name: "request ahead of backend", live: "7", want: "8", expected: pipeline.ActionWait},
		{name: "request behind backend", live: "8", want: "7", expected: pipeline.ActionDrop},
		{name: "unparseable requested version", live: "8", want: "v8", expected: pipeline.ActionWait},
		{name: "unparseable live version", live: "adapter-b", want: "7", expected: pipeline.ActionWait},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate, s := newVersionGate(t)
			if tt.live != "" {
				if err := s.Set(DefaultPolicyVersionKey, tt.live); err != nil {
					t.Fatalf("failed to seed live version: %v", err)
				}
			}

			verdict, err := applyVersion(t, gate, tt.want)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if verdict.Action != tt.expected {
				t.Fatalf("expected action %v, got %v", tt.expected, verdict.Action)
			}
		})
	}
}

// TestPolicyVersionGate_DropCarriesResult asserts a stale rollout comes back to
// the caller as a gate-dropped result naming both versions, rather than
// vanishing silently.
func TestPolicyVersionGate_DropCarriesResult(t *testing.T) {
	gate, s := newVersionGate(t)
	if err := s.Set(DefaultPolicyVersionKey, "9"); err != nil {
		t.Fatalf("failed to seed live version: %v", err)
	}

	verdict, err := applyVersion(t, gate, "3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if verdict.Action != pipeline.ActionDrop {
		t.Fatalf("expected drop, got %v", verdict.Action)
	}
	if verdict.Result == nil {
		t.Fatal("expected a result on the drop verdict, got nil")
	}
	if verdict.Result.ErrorCode != api.ErrCodeGateDropped {
		t.Fatalf("expected error code %q, got %q", api.ErrCodeGateDropped, verdict.Result.ErrorCode)
	}
}

// TestPolicyVersionGate_FailsOpenOnRedisError checks the gate does not stall
// every worker in the pool when Redis is unreachable: the request continues and
// the error is returned for the caller to log.
func TestPolicyVersionGate_FailsOpenOnRedisError(t *testing.T) {
	gate, s := newVersionGate(t)
	if err := s.Set(DefaultPolicyVersionKey, "7"); err != nil {
		t.Fatalf("failed to seed live version: %v", err)
	}
	s.Close()

	verdict, err := applyVersion(t, gate, "8")
	if err == nil {
		t.Fatal("expected an error when Redis is unreachable")
	}
	if verdict.Action != pipeline.ActionContinue {
		t.Fatalf("expected continue on Redis error, got %v", verdict.Action)
	}
}

func TestPolicyVersionGate_BudgetIsOpen(t *testing.T) {
	gate, _ := newVersionGate(t)
	if budget := gate.Budget(context.Background()); budget != 1.0 {
		t.Fatalf("expected an open budget of 1.0, got %v", budget)
	}
}

// TestPolicyVersionGate_PerTenantVersionKey covers the case the single shared
// key cannot serve: two training jobs on one pool, each publishing its own
// snapshot counter. Against a shared key job B's counter of 4 would make every
// job A rollout at 12 look stale; namespaced, each job is judged against its own
// published version.
func TestPolicyVersionGate_PerTenantVersionKey(t *testing.T) {
	tests := []struct {
		name     string
		tenant   string
		want     string
		expected pipeline.VerdictAction
	}{
		{name: "job A matches its own version", tenant: "team-a", want: "12", expected: pipeline.ActionContinue},
		{name: "job B matches its own version", tenant: "team-b", want: "4", expected: pipeline.ActionContinue},
		{name: "job A is not judged against job B", tenant: "team-a", want: "5", expected: pipeline.ActionDrop},
		{name: "job B is not judged against job A", tenant: "team-b", want: "5", expected: pipeline.ActionWait},
		{name: "tenant with no published version", tenant: "team-c", want: "1", expected: pipeline.ActionContinue},
		// Without a tenant the key cannot be resolved, and admitting is the
		// same fail-open the gate takes for a request naming no version.
		{name: "no tenant on the request", tenant: "", want: "1", expected: pipeline.ActionContinue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate, s := newTenantVersionGate(t)
			for tenant, live := range map[string]string{"team-a": "12", "team-b": "4"} {
				if err := s.Set(DefaultPolicyVersionKey+":"+tenant, live); err != nil {
					t.Fatalf("failed to seed live version for %s: %v", tenant, err)
				}
			}

			headers := map[string]string{DefaultPolicyVersionAttribute: tt.want}
			if tt.tenant != "" {
				headers[testTenantAttribute] = tt.tenant
			}
			msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req1", Headers: headers})
			var releases []pipeline.GateReleaseFunc
			verdict, err := gate.Apply(context.Background(), msg, &releases)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if verdict.Action != tt.expected {
				t.Errorf("Action = %v, want %v", verdict.Action, tt.expected)
			}
		})
	}
}

// The async-broker coordinator can only be configured to forward the version as
// an HTTP header, never as metadata, so the header path is the one the
// deployed topology actually exercises.
func TestPolicyVersionGate_ReadsVersionFromHeaders(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		headers  map[string]string
		expected pipeline.VerdictAction
	}{
		{
			name:     "header only, stale",
			headers:  map[string]string{DefaultPolicyVersionAttribute: "7"},
			expected: pipeline.ActionDrop,
		},
		{
			name:     "header only, matching",
			headers:  map[string]string{DefaultPolicyVersionAttribute: "8"},
			expected: pipeline.ActionContinue,
		},
		{
			name:     "header only, ahead",
			headers:  map[string]string{DefaultPolicyVersionAttribute: "9"},
			expected: pipeline.ActionWait,
		},
		{
			name:     "neither set",
			expected: pipeline.ActionContinue,
		},
		{
			// A direct producer setting metadata must not be overridden by a
			// header the coordinator happened to forward.
			name:     "metadata wins over header",
			metadata: map[string]string{DefaultPolicyVersionAttribute: "8"},
			headers:  map[string]string{DefaultPolicyVersionAttribute: "7"},
			expected: pipeline.ActionContinue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate, s := newVersionGate(t)
			if err := s.Set(DefaultPolicyVersionKey, "8"); err != nil {
				t.Fatalf("failed to seed live version: %v", err)
			}

			msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
				ID:       "req1",
				Metadata: tt.metadata,
				Headers:  tt.headers,
			})
			var releases []pipeline.GateReleaseFunc
			verdict, err := gate.Apply(context.Background(), msg, &releases)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if verdict.Action != tt.expected {
				t.Errorf("Action = %v, want %v", verdict.Action, tt.expected)
			}
		})
	}
}
