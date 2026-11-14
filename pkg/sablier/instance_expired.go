package sablier

import (
	"context"
	"errors"
	"log/slog"

	"github.com/sablierapp/sablier/pkg/store"

	"github.com/sablierapp/sablier/pkg/metrics"
)

// OnInstanceExpired returns a store-expiration callback that stops the
// instance via the provider and records the corresponding metrics.
//
// recorder may be metrics.Noop{} when metrics are disabled — call sites
// must always pass a non-nil recorder.
func OnInstanceExpired(ctx context.Context, provider Provider, recorder metrics.Recorder, logger *slog.Logger) func(string) {
	return onInstanceExpired(ctx, provider, recorder, logger, false)
}

func (s *Sablier) OnInstanceExpired(ctx context.Context) func(string) {
	base := onInstanceExpired(ctx, s.provider, s.metrics, s.l, s.verifyEnabledOnExpiration)
	return func(key string) {
		if s.pauseGrace != nil {
			go func() {
				unlock := s.instanceLocks.lock(key)
				defer unlock()
				if _, err := s.sessions.Get(ctx, key); err == nil {
					return
				} else if !errors.Is(err, store.ErrKeyNotFound) {
					s.l.ErrorContext(ctx, "could not verify expired session before pausing instance", slog.String("instance", key), slog.Any("error", err))
					return
				}
				s.markStoppedOnPurpose(key)
				if s.verifyEnabledOnExpiration {
					info, err := s.provider.InstanceInspect(ctx, key)
					if err != nil {
						s.l.WarnContext(ctx, "instance expired could not be inspected before pause", slog.String("instance", key), slog.Any("error", err))
						return
					}
					if !info.IsEnabled() {
						s.l.WarnContext(ctx, "instance expired but is not managed by sablier, skipping pause", slog.String("instance", key))
						return
					}
				}
				if err := s.provider.InstanceStop(ctx, key); err != nil {
					s.l.ErrorContext(ctx, "instance expired could not be paused", slog.String("instance", key), slog.Any("error", err))
					return
				}
				s.l.InfoContext(ctx, "instance paused; grace period started", slog.String("instance", key), slog.Duration("grace_period", s.pauseGrace.duration))
				s.pauseGrace.schedule(key)
			}()
		} else {
			s.markStoppedOnPurpose(key)
			base(key)
		}
		s.triggerAntiAffinityReconcile(ctx)
	}
}

func onInstanceExpired(ctx context.Context, provider Provider, recorder metrics.Recorder, logger *slog.Logger, verifyEnabled bool) func(string) {
	return func(_key string) {
		go func(key string) {
			logger.InfoContext(ctx, "instance expired", slog.String("instance", key))
			if verifyEnabled {
				info, err := provider.InstanceInspect(ctx, key)
				if err != nil {
					logger.WarnContext(ctx, "instance expired could not be inspected before stop", slog.String("instance", key), slog.Any("error", err))
					return
				}
				if !info.IsEnabled() {
					logger.WarnContext(ctx, "instance expired but is not managed by sablier, skipping stop", slog.String("instance", key))
					return
				}
			}
			err := provider.InstanceStop(ctx, key)
			if err != nil {
				logger.ErrorContext(ctx, "instance expired could not be stopped from provider", slog.String("instance", key), slog.Any("error", err))
			}
			recorder.RecordInstanceStop(key, "expired")
			recorder.RecordInactiveInstance(key)
			recorder.DiscardReadyWait(key)
		}(_key)
	}
}
