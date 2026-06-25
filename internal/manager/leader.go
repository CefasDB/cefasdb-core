package manager

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type LeaderElector interface {
	Acquire(ctx context.Context) (LeaderLease, error)
}

type LeaderElectionOptions struct {
	Namespace string
	Name      string
	HolderID  string
	TTL       time.Duration
	Labels    map[string]string
}

type LeaderLease struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	Holder    string    `json:"holder,omitempty"`
	Acquired  bool      `json:"acquired"`
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
}

type KubeLeaderElector struct {
	Client *HTTPKubeClient
	Opts   LeaderElectionOptions
}

func (e *KubeLeaderElector) Acquire(ctx context.Context) (LeaderLease, error) {
	if e == nil || e.Client == nil {
		return LeaderLease{}, errors.New("kubernetes leader elector is not configured")
	}
	opts := e.Opts
	if opts.Namespace == "" {
		return LeaderLease{}, errors.New("leader namespace is required")
	}
	if opts.Name == "" {
		return LeaderLease{}, errors.New("leader lease name is required")
	}
	if opts.HolderID == "" {
		return LeaderLease{}, errors.New("leader holder id is required")
	}
	if opts.TTL <= 0 {
		opts.TTL = 30 * time.Second
	}
	now := e.Client.clock().UTC()
	current, status, err := e.Client.getLease(ctx, opts.Namespace, opts.Name)
	if err != nil {
		return LeaderLease{}, err
	}
	if status == 0 || status == httpStatusNotFound {
		created := Lease{
			Metadata: ObjectMeta{Name: opts.Name, Namespace: opts.Namespace, Labels: cloneStringMap(opts.Labels)},
			Spec: LeaseSpec{
				HolderIdentity:       opts.HolderID,
				LeaseDurationSeconds: int(opts.TTL / time.Second),
				AcquireTime:          now.Format(time.RFC3339Nano),
				RenewTime:            now.Format(time.RFC3339Nano),
			},
		}
		out, err := e.Client.putLease(ctx, opts.Namespace, created, true)
		if err != nil {
			return LeaderLease{}, err
		}
		return leaderLeaseFromKube(out, now, true), nil
	}
	expired := leaseExpired(current, now)
	if current.Spec.HolderIdentity != "" && current.Spec.HolderIdentity != opts.HolderID && !expired {
		return leaderLeaseFromKube(current, now, false), nil
	}
	renewed := current
	if renewed.Metadata.Labels == nil {
		renewed.Metadata.Labels = map[string]string{}
	}
	for k, v := range opts.Labels {
		renewed.Metadata.Labels[k] = v
	}
	if renewed.Spec.HolderIdentity != opts.HolderID {
		renewed.Spec.LeaseTransitions++
		renewed.Spec.AcquireTime = now.Format(time.RFC3339Nano)
	}
	renewed.Spec.HolderIdentity = opts.HolderID
	renewed.Spec.LeaseDurationSeconds = int(opts.TTL / time.Second)
	renewed.Spec.RenewTime = now.Format(time.RFC3339Nano)
	out, err := e.Client.putLease(ctx, opts.Namespace, renewed, false)
	if err != nil {
		return LeaderLease{}, fmt.Errorf("renew leader lease: %w", err)
	}
	return leaderLeaseFromKube(out, now, true), nil
}

const httpStatusNotFound = 404

func leaderLeaseFromKube(l Lease, now time.Time, acquired bool) LeaderLease {
	expires := time.Time{}
	raw := l.Spec.RenewTime
	if raw == "" {
		raw = l.Spec.AcquireTime
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil && l.Spec.LeaseDurationSeconds > 0 {
		expires = t.Add(time.Duration(l.Spec.LeaseDurationSeconds) * time.Second)
	}
	if !expires.IsZero() && now.After(expires) {
		acquired = false
	}
	return LeaderLease{
		Namespace: l.Metadata.Namespace,
		Name:      l.Metadata.Name,
		Holder:    l.Spec.HolderIdentity,
		Acquired:  acquired,
		ExpiresAt: expires,
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
