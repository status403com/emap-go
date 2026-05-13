package emap

import "context"

// Mailbox is the narrow capability surface modules use to watch mail for the
// task they're running. The runner builds one per task at Init, bound to the
// task's resolved IMAP credential. Modules call Watch to stream matching
// messages on a channel; cancelling the ctx closes the underlying
// Subscription so the Manager can refcount the session down to teardown.
//
// One Watch per Mailbox is the expected pattern, though multiple
// simultaneous watches are safe (each becomes a separate Subscription
// sharing the same underlying credential session).
type Mailbox interface {
	Watch(ctx context.Context, f Filter) (<-chan Message, error)
}

// NewMailbox returns a Mailbox that issues subscriptions against mgr using
// cred. The Mailbox itself is cheap to construct — no IMAP work happens
// until Watch is called.
func NewMailbox(mgr *Manager, cred Credential) Mailbox {
	return &credBoundMailbox{mgr: mgr, cred: cred}
}

type credBoundMailbox struct {
	mgr  *Manager
	cred Credential
}

func (b *credBoundMailbox) Watch(ctx context.Context, f Filter) (<-chan Message, error) {
	sub, err := b.mgr.Subscribe(b.cred, f)
	if err != nil {
		return nil, err
	}
	// Drop a tiny watcher goroutine that ties the Subscription's lifetime to
	// the caller's context. Cheaper than asking every module to remember to
	// defer sub.Close() — the Module interface already runs with a ctx.
	go func() {
		<-ctx.Done()
		sub.Close()
	}()
	return sub.Ch, nil
}
