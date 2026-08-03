package pr

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type actionFakeStore struct {
	prs      []domain.PullRequest
	owner    domain.SessionRecord
	ownerOK  bool
	listErr  error
	ownerErr error
}

func (f *actionFakeStore) ListPRsByNumber(context.Context, int) ([]domain.PullRequest, error) {
	return append([]domain.PullRequest(nil), f.prs...), f.listErr
}
func (f *actionFakeStore) GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error) {
	return f.owner, f.ownerOK, f.ownerErr
}

type actionFakeProvider struct {
	observations []ports.SCMObservation
	fetchErr     error
	fetchErrors  []error
	fetchCalls   int
	review       ports.SCMReviewObservation
	reviewErr    error
	mutation     ports.SCMMergeResult
	mutationErr  error
	mergeCalls   int
	expectedHead string
	ref          ports.SCMPRRef
}

func (f *actionFakeProvider) FetchPullRequests(context.Context, []ports.SCMPRRef) ([]ports.SCMObservation, error) {
	call := f.fetchCalls
	f.fetchCalls++
	if call < len(f.fetchErrors) && f.fetchErrors[call] != nil {
		return nil, f.fetchErrors[call]
	}
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	if len(f.observations) == 0 {
		return nil, nil
	}
	got := f.observations[0]
	f.observations = f.observations[1:]
	return []ports.SCMObservation{got}, nil
}
func (f *actionFakeProvider) FetchReviewThreads(context.Context, ports.SCMPRRef) (ports.SCMReviewObservation, error) {
	return f.review, f.reviewErr
}
func (f *actionFakeProvider) MergePullRequest(_ context.Context, ref ports.SCMPRRef, expectedHead string) (ports.SCMMergeResult, error) {
	f.mergeCalls++
	f.ref, f.expectedHead = ref, expectedHead
	return f.mutation, f.mutationErr
}

func actionFixture() (*actionFakeStore, *actionFakeProvider) {
	store := &actionFakeStore{
		prs: []domain.PullRequest{{
			URL: "https://github.com/acme/widgets/pull/42", SessionID: "widgets-1", Number: 42,
			Provider: "github", Host: "github.com", Repo: "acme/widgets", HeadSHA: "head-abc",
		}},
		owner: domain.SessionRecord{ID: "widgets-1"}, ownerOK: true,
	}
	provider := &actionFakeProvider{
		observations: []ports.SCMObservation{readyActionObservation(false), readyActionObservation(true)},
		mutation:     ports.SCMMergeResult{Merged: true, MergeCommitSHA: "merge-def"},
	}
	return store, provider
}

func readyActionObservation(merged bool) ports.SCMObservation {
	obs := ports.SCMObservation{
		Fetched: true, Provider: "github", Host: "github.com", Repo: "acme/widgets",
		PR:           ports.SCMPRObservation{Number: 42, HeadSHA: "head-abc", Merged: merged},
		CI:           ports.SCMCIObservation{Summary: string(domain.CIPassing), HeadSHA: "head-abc"},
		Review:       ports.SCMReviewObservation{Decision: string(domain.ReviewNone)},
		Mergeability: ports.SCMMergeabilityObservation{State: string(domain.MergeMergeable)},
	}
	if merged {
		obs.PR.MergeCommitSHA = "merge-def"
	}
	return obs
}

func TestActionServiceMergeGuardsAndConfirmsLiveOutcome(t *testing.T) {
	store, provider := actionFixture()
	got, err := NewActionService(store, provider).Merge(context.Background(), "42")
	if err != nil {
		t.Fatal(err)
	}
	if got.PRNumber != 42 || got.Method != "squash" || got.HeadSHA != "head-abc" || got.MergeCommitSHA != "merge-def" {
		t.Fatalf("result = %#v", got)
	}
	if provider.mergeCalls != 1 || provider.expectedHead != "head-abc" || provider.ref.Repo.Repo != "acme/widgets" {
		t.Fatalf("mutation evidence = calls:%d head:%q ref:%#v", provider.mergeCalls, provider.expectedHead, provider.ref)
	}
}

func TestActionServiceMergeConfirmsOutcomeAfterIndeterminateMutationError(t *testing.T) {
	store, provider := actionFixture()
	provider.mutationErr = errors.Join(ports.ErrSCMMergeOutcomeUnknown, errors.New("connection dropped after PUT"))
	provider.mutation = ports.SCMMergeResult{}
	got, err := NewActionService(store, provider).Merge(context.Background(), "42")
	if err != nil {
		t.Fatal(err)
	}
	if got.HeadSHA != "head-abc" || got.MergeCommitSHA != "merge-def" || provider.fetchCalls != 2 {
		t.Fatalf("result = %#v, fetch calls = %d", got, provider.fetchCalls)
	}
}

func TestActionServiceMergeRejectsDefinitiveMutationErrorDespiteConcurrentMerge(t *testing.T) {
	store, provider := actionFixture()
	provider.mutationErr = errors.New("github rejected merge with 409")
	provider.mutation = ports.SCMMergeResult{}
	_, err := NewActionService(store, provider).Merge(context.Background(), "42")
	if !errors.Is(err, ErrPRProvider) {
		t.Fatalf("err = %v, want provider failure", err)
	}
	if provider.fetchCalls != 1 {
		t.Fatalf("fetch calls = %d, want preflight only; concurrent merged readback must not recover a rejection", provider.fetchCalls)
	}
}

func TestActionServiceMergeReadbackFailureIsMismatch(t *testing.T) {
	store, provider := actionFixture()
	provider.fetchErrors = []error{nil, errors.New("readback unavailable")}
	_, err := NewActionService(store, provider).Merge(context.Background(), "42")
	if !errors.Is(err, ErrPRMergeMismatch) || !errors.Is(err, ErrPRProvider) {
		t.Fatalf("err = %v, want merge mismatch carrying provider cause", err)
	}
}

func TestActionServiceMergeFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		want error
		edit func(*actionFakeStore, *actionFakeProvider)
	}{
		{"absent ownership", ErrPRNotFound, func(s *actionFakeStore, _ *actionFakeProvider) { s.prs = nil }},
		{"ambiguous ownership", ErrPRAmbiguous, func(s *actionFakeStore, _ *actionFakeProvider) { s.prs = append(s.prs, s.prs[0]) }},
		{"inactive ownership", ErrPROwnerInactive, func(s *actionFakeStore, _ *actionFakeProvider) { s.owner.IsTerminated = true }},
		{"stale head", ErrPRHeadChanged, func(_ *actionFakeStore, p *actionFakeProvider) { p.observations[0].PR.HeadSHA = "drift" }},
		{"ci not passing", ErrPRPreconditions, func(_ *actionFakeStore, p *actionFakeProvider) {
			p.observations[0].CI.Summary = string(domain.CIPending)
		}},
		{"review changes requested", ErrPRPreconditions, func(_ *actionFakeStore, p *actionFakeProvider) {
			p.review.Decision = string(domain.ReviewChangesRequest)
		}},
		{"review evidence partial", ErrPRPreconditions, func(_ *actionFakeStore, p *actionFakeProvider) { p.review.Partial = true }},
		{"unresolved thread", ErrPRPreconditions, func(_ *actionFakeStore, p *actionFakeProvider) {
			p.review.Threads = []ports.SCMReviewThreadObservation{{Resolved: false}}
		}},
		{"not mergeable", ErrPRNotMergeable, func(_ *actionFakeStore, p *actionFakeProvider) {
			p.observations[0].Mergeability.State = string(domain.MergeBlocked)
		}},
		{"provider fetch failure", ErrPRProvider, func(_ *actionFakeStore, p *actionFakeProvider) { p.fetchErr = errors.New("offline") }},
		{"provider mutation failure", ErrPRProvider, func(_ *actionFakeStore, p *actionFakeProvider) {
			p.mutationErr = errors.New("rejected")
			p.observations[1] = readyActionObservation(false)
		}},
		{"mutation mismatch", ErrPRMergeMismatch, func(_ *actionFakeStore, p *actionFakeProvider) { p.mutation.Merged = false }},
		{"readback mismatch", ErrPRMergeMismatch, func(_ *actionFakeStore, p *actionFakeProvider) { p.observations[1].PR.MergeCommitSHA = "other" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, provider := actionFixture()
			tt.edit(store, provider)
			_, err := NewActionService(store, provider).Merge(context.Background(), "42")
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if !errors.Is(tt.want, ErrPRProvider) && !errors.Is(tt.want, ErrPRMergeMismatch) && provider.mergeCalls != 0 {
				t.Fatalf("merge calls = %d, want 0", provider.mergeCalls)
			}
		})
	}
}
