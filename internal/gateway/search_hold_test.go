package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

const heldSearch = "data: " + `{"type":"response.web_search_call.searching","item_id":"ws_held","output_index":0}` + "\n\n"

// Search establishes that the upstream has started, but commits no
// response identity or content and doesn't count as a text/reasoning token.
func TestHeldSearchStartsWithoutCommitting(t *testing.T) {
	rec := httptest.NewRecorder()
	h := newHoldWriter(rec, true)
	h.alive, h.streams = &keptAlive{proto: provider.Responses}, true
	h.firstWait = time.Second
	h.Header().Set("Content-Type", "text/event-stream")
	io.WriteString(h, quietCreated)
	if h.started() {
		t.Fatal("a protocol frame satisfied FirstToken")
	}
	io.WriteString(h, heldSearch)
	if !h.started() || !h.mayAskAgain() || rec.Body.Len() != 0 {
		t.Fatalf("search didn't stay uncommitted: started=%v retry=%v body=%q", h.started(), h.mayAskAgain(), rec.Body.String())
	}
	if ttft, text := h.first.ms(); ttft != 0 || text != 0 {
		t.Fatalf("search counted as a token: TTFT=%d FirstText=%d", ttft, text)
	}
	io.WriteString(h, quietDelta("answer"))
	if !strings.Contains(rec.Body.String(), heldSearch) || !strings.Contains(rec.Body.String(), "answer") || h.mayAskAgain() {
		t.Fatalf("content didn't release the search: %q", rec.Body.String())
	}
}

// None of the limit paths may flush a search just to bound its hold:
// a later overload would then become the client's failure again.
func TestHeldSearchLimitsDoNotRelease(t *testing.T) {
	for _, kind := range []string{"bytes", "time/write", "time/quiet", "idle/write", "idle/quiet"} {
		t.Run(kind, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h := newHoldWriter(rec, true)
			h.ctx = context.Background()
			h.alive, h.streams = &keptAlive{proto: provider.Responses, streams: true}, true
			h.Header().Set("Content-Type", "text/event-stream")
			stops := 0
			h.stop = func() { stops++ }
			io.WriteString(h, quietCreated+heldSearch)
			switch {
			case kind == "bytes":
				_, _ = io.WriteString(h, ": "+strings.Repeat("x", holdMost)+"\n\n")
			case strings.HasPrefix(kind, "time"):
				h.since = time.Now().Add(-holdLead - time.Second)
			default:
				h.alive.told = time.Now().Add(-agentQuietMost - time.Second)
			}
			if strings.HasSuffix(kind, "/quiet") {
				h.keepQuiet()
			} else if kind != "bytes" {
				_, _ = io.WriteString(h, ": ping\n\n")
			}
			status := http.StatusGatewayTimeout
			if kind == "bytes" {
				status = http.StatusBadGateway
			}
			if stops != 1 || h.code() != status || !h.heldLimit || rec.Body.Len() != 0 {
				t.Fatalf("limit: canceled=%d status=%d local=%v sent=%d bytes", stops, h.code(), h.heldLimit, rec.Body.Len())
			}
			before := h.held.Len()
			if _, err := io.WriteString(h, quietDelta("late")); !errors.Is(err, errHeldLimit) {
				t.Fatalf("a canceled search accepted another write: %v", err)
			}
			h.release()
			if h.held.Len() != before || rec.Body.Len() != 0 {
				t.Fatal("a canceled search appended or released bytes")
			}
		})
	}
}

// Once the vendor goes silent, no further Write can enforce the idle
// limit. The existing watch must end this held attempt without flushing it.
func TestHeldSearchWatchEndsSilence(t *testing.T) {
	for _, partial := range []string{"", `data: {"type":"response.completed","response":`, `data: {"type":"response.incomplete","response":`} {
		t.Run(partial, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h := newHoldWriter(rec, true)
			h.ctx = context.Background()
			h.alive, h.streams = &keptAlive{proto: provider.Responses, streams: true}, true
			h.Header().Set("Content-Type", "text/event-stream")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h.stop = cancel
			io.WriteString(h, quietCreated+heldSearch+partial)
			h.keepAlive()
			h.alive.told = time.Now().Add(-agentQuietMost - time.Second)
			stopWatch := h.watch()
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				stopWatch()
				t.Fatal("silent held search was not canceled by watch")
			}
			stopWatch()
			if h.code() != 504 || !h.heldLimit || strings.Contains(rec.Body.String(), "data:") {
				t.Fatalf("watch released the search: status=%d local=%v body=%q", h.code(), h.heldLimit, rec.Body.String())
			}
		})
	}
}

type searchCancelWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w searchCancelWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(b)
	if strings.Contains(string(b), ": keepalive") {
		w.cancel()
	}
	return n, err
}

func (w searchCancelWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

type searchCancelTail struct {
	io.Reader
	tail *strings.Reader
}

func (b *searchCancelTail) Read(p []byte) (int, error) {
	if b.Reader != nil {
		n, err := b.Reader.Read(p)
		if err != io.EOF {
			return n, err
		}
		b.Reader = nil
	}
	return b.tail.Read(p)
}

// A terminal marker isn't a complete SSE event. If the agent cancels
// while it is still held, record cancellation rather than a successful
// reply, and don't send the abandoned search or ask another candidate.
func TestHeldSearchPartialEndCanceled(t *testing.T) {
	for _, end := range []string{"response.completed", "response.incomplete"} {
		t.Run(end, func(t *testing.T) {
			fresh(t)
			for _, id := range []string{"a", "b"} {
				if err := provider.Save(provider.Provider{ID: id, Name: id, Key: "fixture", Models: []string{"m"}, Responses: "http://" + id + ".invalid/v1"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := provider.SaveGroup(provider.Group{Name: "G", Members: []string{"a/m", "b/m"}, Routing: provider.Ordered}); err != nil {
				t.Fatal(err)
			}
			old := keepHeldAfter
			keepHeldAfter = -time.Second
			t.Cleanup(func() { keepHeldAfter = old })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := New()
			asked := 0
			s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				asked++
				if r.URL.Host != "a.invalid" {
					t.Errorf("canceled request tried %s", r.URL.Host)
				}
				wire := quietCreated + heldSearch + `data: {"type":"` + end + `","response":`
				body := &searchCancelTail{Reader: strings.NewReader(wire), tail: strings.NewReader(`{"status":"completed"}}` + "\n\n")}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(body), Request: r}, nil
			})}
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(searchCancelWriter{rec, cancel}, httptest.NewRequest("POST", "/v1/responses", strings.NewReader(codexAsk)).WithContext(ctx))
			rt := s.trace.routes[len(s.trace.routes)-1]
			if asked != 1 || rt.Status != 499 || len(rt.Tries) != 1 || rt.Tries[0].Fail != failCanceled || rt.Tries[0].Rest != nil {
				t.Fatalf("held search canceled as success/fallback: asked=%d route=%+v", asked, rt)
			}
			if strings.Contains(rec.Body.String(), "data:") {
				t.Fatalf("canceled search reached the client: %q", rec.Body.String())
			}
		})
	}
}

// A real content event releases before a large following chunk, whether
// those events arrived together or on separate writes.
func TestHeldSearchContentBeforeLargeWrite(t *testing.T) {
	content := quietDelta("answer")
	padding := ": " + strings.Repeat("x", holdMost) + "\n\n"
	for _, together := range []bool{false, true} {
		rec := httptest.NewRecorder()
		h := newHoldWriter(rec, true)
		h.alive, h.streams = &keptAlive{proto: provider.Responses}, true
		h.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(h, heldSearch)
		if together {
			io.WriteString(h, content+padding)
		} else {
			io.WriteString(h, content)
			io.WriteString(h, padding)
		}
		if rec.Body.String() != heldSearch+content+padding || h.heldLimit {
			t.Fatalf("content was limited (together=%v): status=%d size=%d", together, h.code(), rec.Body.Len())
		}
	}
}

func TestHeldSearchOversizedEventDoesNotDependOnWrites(t *testing.T) {
	event := quietDelta(strings.Repeat("x", holdMost))
	for _, split := range []bool{false, true} {
		rec := httptest.NewRecorder()
		h := newHoldWriter(rec, true)
		h.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(h, heldSearch)
		if split {
			_, _ = io.WriteString(h, event[:holdMost])
			_, _ = io.WriteString(h, event[holdMost:])
		} else {
			_, _ = io.WriteString(h, event)
		}
		if !h.heldLimit || rec.Body.Len() != 0 {
			t.Fatalf("oversized event was released (split=%v): local=%v bytes=%d", split, h.heldLimit, rec.Body.Len())
		}
	}
}

// These neighboring hold modes retain their old limit policies.
func TestHeldSearchLeavesOtherHoldPolicies(t *testing.T) {
	for _, mode := range []string{"frames", "whole", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h := newHoldWriter(rec, true)
			h.alive, h.streams = &keptAlive{proto: provider.Responses}, true
			h.Header().Set("Content-Type", "text/event-stream")
			if mode == "whole" {
				h.wantWhole = true
			}
			io.WriteString(h, quietCreated)
			if mode != "frames" {
				io.WriteString(h, heldSearch)
			}
			if mode == "canceled" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				h.ctx = ctx
			}
			h.since = time.Now().Add(-holdLead - time.Second)
			h.keepQuiet()
			if mode != "canceled" {
				io.WriteString(h, ": ping\n\n")
			}
			if h.heldLimit {
				t.Fatal("a different hold policy became a local search failure")
			}
			if mode == "frames" && !h.passing || mode == "whole" && h.passing {
				t.Fatalf("changed %s release policy: passing=%v", mode, h.passing)
			}
		})
	}
}

// Exercise the gateway's local-cancellation result, including its last
// candidate and already-started keepalive response, rather than only the
// writer. Local limits must neither rest nor repeatedly replan a member.
func TestHeldSearchLimitRoutesWithoutRest(t *testing.T) {
	for _, last := range []bool{false, true} {
		for _, kept := range []bool{false, true} {
			t.Run(strings.Join([]string{map[bool]string{false: "next", true: "last"}[last], map[bool]string{false: "headers", true: "keepalive"}[kept]}, "/"), func(t *testing.T) {
				fresh(t)
				var a atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.Copy(io.Discard, r.Body)
					a.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, quietCreated+heldSearch)
					w.(http.Flusher).Flush()
					io.WriteString(w, ": "+strings.Repeat("x", holdMost)+"\n\n")
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
						t.Error("held search was not canceled at its buffer limit")
					}
				}))
				t.Cleanup(up.Close)
				if err := provider.Save(provider.Provider{ID: "a", Name: "A", Key: "fixture", Models: []string{"m"}, Responses: up.URL + "/v1"}); err != nil {
					t.Fatal(err)
				}
				b := &scripted{replies: []reply{{200, "text/event-stream", quietCreated + quietDelta("winner") + quietDone}}}
				responsesOn(t, "b", b)
				members := []string{"a/m"}
				if !last {
					members = append(members, "b/m")
				}
				if err := provider.SaveGroup(provider.Group{Name: "G", Members: members, Routing: provider.Ordered, Patience: 240}); err != nil {
					t.Fatal(err)
				}
				if kept {
					old := keepHeldAfter
					keepHeldAfter = -time.Second
					t.Cleanup(func() { keepHeldAfter = old })
				}
				s := New()
				code, body := sendTo(s, "/v1/responses", codexAsk)
				if a.Load() != 1 || strings.Contains(body, "ws_held") {
					t.Fatalf("local limit retried or leaked: a=%d body=%q", a.Load(), body)
				}
				rt := s.trace.routes[len(s.trace.routes)-1]
				if rt.Tries[0].Fail != failHeldLimit || rt.Tries[0].Status != 502 || rt.Tries[0].Rest != nil || rt.Tries[0].Again != 0 || rt.Tries[0].Replan != 0 {
					t.Fatalf("local limit treated as a provider failure: %+v", rt.Tries)
				}
				if last {
					wantStatus := 502
					if kept {
						wantStatus = 200
					}
					if b.n != 0 || len(rt.Tries) != 1 || code != wantStatus || !strings.Contains(body, "buffer size limit") {
						t.Fatalf("last local failure: %d %q tries=%+v b=%d", code, body, rt.Tries, b.n)
					}
				} else if code != 200 || !strings.Contains(body, "winner") || b.n != 1 || len(rt.Tries) != 2 {
					t.Fatalf("next candidate didn't answer: %d %q tries=%+v b=%d", code, body, rt.Tries, b.n)
				}
			})
		}
	}
}
