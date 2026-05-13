// Package emap is an efficient IMAP client built for high-fanout
// concurrent mail watching.
//
// emap (Efficient IMAP) is designed for workloads where many independent
// tasks need to receive mail from a small set of inboxes — for example,
// thousands of automation tasks all reading verification mail from a
// shared catch-all mailbox. The core promise: one persistent IMAP
// connection per credential, fanned out to N subscribers, regardless of
// how many tasks are running.
//
// # Quick start
//
//	mgr := emap.NewManager(emap.DefaultLinger)
//	defer mgr.Shutdown()
//
//	cred := emap.Credential{
//		Host:     "imap.gmail.com",
//		Port:     993,
//		UseTLS:   true,
//		Email:    "you@example.com",
//		Password: "app-specific-password",
//	}
//
//	sub, err := mgr.Subscribe(cred, emap.Filter{
//		To:           "verify+task@example.com",
//		FromContains: "no-reply@vendor",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer sub.Close()
//
//	select {
//	case msg := <-sub.Ch:
//		fmt.Printf("got verification mail: %s\n", msg.Body)
//	case <-time.After(60 * time.Second):
//		log.Println("timeout waiting for mail")
//	}
//
// # Design highlights
//
//   - Per-credential connection pooling: 2000 tasks reading the same
//     catch-all share a single IMAP connection, not 2000.
//   - UIDNEXT-based backlog skip: connecting to an inbox with a million
//     stored messages costs the same as an empty one. emap never fetches
//     mail older than the moment your session started.
//   - IDLE-first: pushes from the server, not polling, when available.
//     Falls back to configurable polling when the server doesn't support
//     IDLE.
//   - Hand-rolled wire protocol: no third-party IMAP dependency, only
//     stdlib + net/mail. Small surface area for the memory leaks that
//     plague larger IMAP libraries.
//   - Reconnect with exponential backoff on transport errors; distinguishes
//     protocol-level rejection (BAD/NO) and exits cleanly rather than
//     hammering a server that won't accept the command.
//   - Drop-oldest dispatch under per-subscriber backpressure: a hung
//     consumer cannot pin memory or stall the pipeline.
//
// See README.md and ARCHITECTURE.md in the module root for details.
package emap
