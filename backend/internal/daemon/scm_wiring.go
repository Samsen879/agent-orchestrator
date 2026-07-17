package daemon

// This file wires the provider-neutral SCM observer into daemon startup using
// the GitHub provider for v1. It keeps provider setup non-blocking for readiness
// by resolving tokens lazily inside the background observer path.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	scmgithub "github.com/aoagents/agent-orchestrator/backend/internal/adapters/scm/github"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	scmobserve "github.com/aoagents/agent-orchestrator/backend/internal/observe/scm"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// startSCMObserver wires the provider-neutral SCM observer with the GitHub
// provider used by v1. Missing credentials do not fail daemon startup; the
// observer performs a lazy credential check in its background goroutine, logs
// one warning, and disables itself before any provider API calls.
func startSCMObserver(ctx context.Context, store *sqlite.Store, lcm *lifecycle.Manager, logger *slog.Logger) <-chan struct{} {
	provider, err := newGitHubSCMProvider(logger)
	if err != nil {
		logSCMProviderDisabled(logger, err)
		return closedDone()
	}
	lcm.SetReactionPRResolver(githubReactionPRResolver{provider: provider})
	observer := scmobserve.New(provider, store, lcm, scmobserve.Config{Logger: logger})
	return observer.Start(ctx)
}

type githubReactionPRResolver struct {
	provider *scmgithub.Provider
}

func (r githubReactionPRResolver) ResolveReactionPR(ctx context.Context, reaction domain.LifecycleReaction) (domain.LifecycleReactionPRTarget, error) {
	repo, ok := r.provider.ParseRepository(reaction.Repo)
	if !ok {
		return domain.LifecycleReactionPRTarget{}, fmt.Errorf("resolve lifecycle reaction PR: invalid repository %q", reaction.Repo)
	}
	observations, err := r.provider.FetchPullRequests(ctx, []ports.SCMPRRef{{Repo: repo, Number: reaction.PRNumber, URL: reaction.PRURL}})
	if err != nil {
		return domain.LifecycleReactionPRTarget{}, err
	}
	if len(observations) == 0 || !observations[0].Fetched {
		return domain.LifecycleReactionPRTarget{}, nil
	}
	o := observations[0]
	return domain.LifecycleReactionPRTarget{
		Found: true, URL: firstNonEmpty(o.PR.URL, o.PR.HTMLURL), Number: o.PR.Number,
		Repo: o.Repo, SourceBranch: o.PR.SourceBranch, HeadSHA: o.PR.HeadSHA,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func newGitHubSCMProvider(logger *slog.Logger) (*scmgithub.Provider, error) {
	tokens := scmgithub.FallbackTokenSource{
		scmgithub.EnvTokenSource{EnvVars: []string{"AO_GITHUB_TOKEN"}},
		&scmgithub.GHTokenSource{},
	}
	// Avoid token preflight on daemon startup and session service construction.
	// GHTokenSource may shell out to `gh`, which is too slow/flaky for the startup
	// readiness path. Provider calls resolve credentials lazily when claim-pr or
	// the background observer actually needs GitHub.
	return scmgithub.NewProvider(scmgithub.ProviderOptions{Token: tokens, SkipTokenPreflight: true, Logger: logger})
}

func logSCMProviderDisabled(logger *slog.Logger, err error) {
	if errors.Is(err, scmgithub.ErrNoToken) || errors.Is(err, scmgithub.ErrAuthFailed) {
		logger.Warn("scm observer disabled: no usable GitHub token", "err", err)
	} else {
		logger.Warn("scm observer disabled: GitHub provider setup failed", "err", err)
	}
}

func closedDone() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
