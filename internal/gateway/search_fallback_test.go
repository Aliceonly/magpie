package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/yetone/magpie/internal/provider"
	"github.com/yetone/magpie/internal/settings"
)

const searchFallbackAsk = `{"model":"group/g","stream":true,"store":false,"input":[{"role":"user","content":[{"type":"input_text","text":"Find the search fallback fixture."}]}],"tools":[{"type":"web_search"}],"include":["web_search_call.action.sources"]}`

const searchFallbackCreated = `{"type":"response.created","response":{"id":"resp_search_a","model":"m","status":"in_progress","output":[]}}`
const searchFallbackAdded = `{"type":"response.output_item.added","output_index":0,"item":{"id":"ws_search_a","type":"web_search_call","status":"in_progress"}}`
const searchFallbackInProgress = `{"type":"response.web_search_call.in_progress","output_index":0,"item_id":"ws_search_a"}`
const searchFallbackSearching = `{"type":"response.web_search_call.searching","output_index":0,"item_id":"ws_search_a"}`
const searchFallbackCompleted = `{"type":"response.web_search_call.completed","output_index":0,"item_id":"ws_search_a"}`
const searchFallbackDone = `{"type":"response.output_item.done","output_index":0,"item":{"id":"ws_search_a","type":"web_search_call","status":"completed","action":{"type":"search","query":"query from a","sources":[{"type":"url","url":"https://source-a.invalid/result"}]}}}`
const searchFallbackFailed = `{"type":"response.failed","response":{"id":"resp_search_a","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`
const searchFallbackCapacity = `{"type":"response.failed","response":{"id":"resp_search_a","status":"failed","error":{"code":"server_is_overloaded","message":"Selected model is at capacity. Please try a different model."}}}`

func searchFallbackSSE(data ...string) string {
	var b strings.Builder
	for _, d := range data {
		b.WriteString("data: " + d + "\n\n")
	}
	return b.String()
}

func searchFallbackAnswer() string {
	return searchFallbackSSE(
		`{"type":"response.created","response":{"id":"resp_answer_b","model":"m","status":"in_progress","output":[]}}`,
		`{"type":"response.output_text.delta","item_id":"msg_answer_b","output_index":0,"content_index":0,"delta":"answer from b"}`,
		`{"type":"response.completed","response":{"id":"resp_answer_b","model":"m","status":"completed","output":[{"id":"msg_answer_b","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer from b","annotations":[]}]}],"usage":{"input_tokens":11,"output_tokens":3}}}`,
	)
}

type searchFallbackUpstream struct {
	mu     sync.Mutex
	bodies []string
	chunks []string
	rounds []reply
}

func (u *searchFallbackUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
		http.NotFound(w, r)
		return
	}
	body, _ := io.ReadAll(r.Body)
	u.mu.Lock()
	u.bodies = append(u.bodies, string(body))
	n := len(u.bodies)
	u.mu.Unlock()
	if len(u.rounds) != 0 {
		if n > len(u.rounds) {
			http.Error(w, `{"error":{"message":"unexpected extra search round"}}`, 500)
			return
		}
		reply := u.rounds[n-1]
		w.Header().Set("Content-Type", reply.ctype)
		w.WriteHeader(reply.code)
		io.WriteString(w, reply.body)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	for _, chunk := range u.chunks {
		if _, err := io.WriteString(w, chunk); err != nil {
			return
		}
		w.(http.Flusher).Flush()
	}
}

func (u *searchFallbackUpstream) requests() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.bodies...)
}

func searchFallbackOn(t *testing.T, id string, chunks ...string) *searchFallbackUpstream {
	t.Helper()
	u := &searchFallbackUpstream{chunks: chunks}
	searchFallbackServe(t, id, u, true)
	return u
}

func searchFallbackServe(t *testing.T, id string, u *searchFallbackUpstream, searches bool) {
	t.Helper()
	up := httptest.NewServer(u)
	t.Cleanup(up.Close)
	if err := provider.Save(provider.Provider{
		ID: id, Name: strings.ToUpper(id), Key: "fixture", Models: []string{"m"},
		Responses: up.URL + "/v1", Searches: searches,
	}); err != nil {
		t.Fatal(err)
	}
}

func searchFallbackGroup(t *testing.T) {
	t.Helper()
	if err := provider.SaveGroup(provider.Group{
		Name: "G", Members: []string{"a/m", "b/m"}, Routing: provider.Ordered, Patience: provider.PatienceOff,
	}); err != nil {
		t.Fatal(err)
	}
}

func searchFallbackCleanRequest(t *testing.T, body, ask string) {
	t.Helper()
	var got, original map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(ask), &original); err != nil {
		t.Fatal(err)
	}
	original["model"] = "m"
	if !reflect.DeepEqual(got, original) {
		t.Errorf("candidate did not receive the original request: %s", body)
	}
	for _, leaked := range []string{"resp_search_a", "ws_search_a", "query from a", "source-a.invalid", "server_is_overloaded"} {
		if strings.Contains(body, leaked) {
			t.Errorf("failed search %q contaminated the next request: %s", leaked, body)
		}
	}
}

func searchFallbackRoute(t *testing.T, s *Server, tries int) Route {
	t.Helper()
	routes := s.Trace(context.Background(), 0, 0).Routes
	if len(routes) != 1 {
		t.Fatalf("routes: %+v", routes)
	}
	r := routes[0]
	if !r.Done || len(r.Tries) != tries {
		t.Fatalf("route did not finish with %d tries: %+v", tries, r)
	}
	return r
}

func searchFallbackHandedOver(t *testing.T, s *Server, code int, body string, a, b *searchFallbackUpstream) {
	t.Helper()
	aRequests, bRequests := a.requests(), b.requests()
	if code != http.StatusOK || len(aRequests) != 1 || len(bRequests) != 1 || !strings.Contains(body, "answer from b") {
		t.Fatalf("search-only overload did not fail over: status=%d requests a=%d b=%d body=%s", code, len(aRequests), len(bRequests), body)
	}
	for _, leaked := range []string{"resp_search_a", "ws_search_a", "query from a", "source-a.invalid", "server_is_overloaded", "Selected model is at capacity", "response.failed"} {
		if strings.Contains(body, leaked) {
			t.Errorf("failed search %q leaked to the client: %s", leaked, body)
		}
	}
	searchFallbackCleanRequest(t, aRequests[0], searchFallbackAsk)
	searchFallbackCleanRequest(t, bRequests[0], searchFallbackAsk)
	r := searchFallbackRoute(t, s, 2)
	if r.Status != 200 || r.Error != "" {
		t.Errorf("successful route: %+v", r)
	}
	first, last := r.Tries[0], r.Tries[1]
	if first.Status != 529 || first.Fail != failOther || first.Rest == nil || last.Status != 200 || last.Error != "" || last.Fail != "" {
		t.Errorf("search-only overload and answering try: %+v", r.Tries)
	}
	if first.TTFT != 0 || first.FirstText != 0 || last.TTFT == 0 || last.FirstText == 0 {
		t.Errorf("search progress counted as an answer's first token: %+v", r.Tries)
	}
}

// Search progress has no answer to preserve when an overloaded member
// gives way. The next member gets the original request and owns the reply.
func TestResponsesHostedSearchFailsOver(t *testing.T) {
	for _, phase := range []struct {
		name    string
		data    []string
		failure string
	}{
		{"added", []string{searchFallbackAdded}, searchFallbackFailed},
		{"progress", []string{searchFallbackAdded, searchFallbackInProgress, searchFallbackSearching}, searchFallbackCapacity},
		{"completed", []string{searchFallbackAdded, searchFallbackInProgress, searchFallbackSearching, searchFallbackCompleted, searchFallbackDone}, searchFallbackFailed},
	} {
		for _, writes := range []string{"combined", "events", "fragmented"} {
			t.Run(phase.name+"/"+writes, func(t *testing.T) {
				fresh(t)
				data := append([]string{searchFallbackCreated}, phase.data...)
				data = append(data, phase.failure)
				wire := searchFallbackSSE(data...)
				chunks := []string{wire}
				switch writes {
				case "events":
					chunks = nil
					for _, event := range data {
						chunks = append(chunks, searchFallbackSSE(event))
					}
				case "fragmented":
					cut := strings.Index(wire, `"web_search_call"`) + len(`"web_`)
					chunks = []string{wire[:cut], wire[cut:]}
				}
				a := searchFallbackOn(t, "a", chunks...)
				b := searchFallbackOn(t, "b", searchFallbackAnswer())
				searchFallbackGroup(t)
				s := New()
				code, body := sendTo(s, "/v1/responses", searchFallbackAsk)
				searchFallbackHandedOver(t, s, code, body, a, b)
			})
		}
	}
}

func searchFallbackSuccess(text bool) string {
	output := `[{"id":"ws_search_a","type":"web_search_call","status":"completed","action":{"type":"search","query":"query from a","sources":[{"type":"url","url":"https://source-a.invalid/result"}]}}`
	wire := searchFallbackSSE(searchFallbackCreated, searchFallbackAdded, searchFallbackInProgress, searchFallbackSearching, searchFallbackCompleted, searchFallbackDone)
	if text {
		wire += searchFallbackSSE(`{"type":"response.output_text.delta","output_index":1,"item_id":"msg_search_a","content_index":0,"delta":"answer from a"}`)
		output += `,{"id":"msg_search_a","type":"message","role":"assistant","phase":"final_answer","status":"completed","content":[{"type":"output_text","text":"answer from a","annotations":[]}]}`
	}
	output += `]`
	return wire + searchFallbackSSE(`{"type":"response.completed","response":{"id":"resp_search_a","model":"m","status":"completed","output":`+output+`,"usage":{"input_tokens":13,"output_tokens":5}}}`)
}

func TestResponsesHostedSearchSuccessPreserved(t *testing.T) {
	for _, text := range []bool{false, true} {
		name := "search_only"
		if text {
			name = "with_answer"
		}
		t.Run(name, func(t *testing.T) {
			fresh(t)
			wire := searchFallbackSuccess(text)
			a := searchFallbackOn(t, "a", wire)
			b := searchFallbackOn(t, "b", searchFallbackAnswer())
			searchFallbackGroup(t)
			s := New()
			code, body := sendTo(s, "/v1/responses", searchFallbackAsk)
			if code != http.StatusOK || len(a.requests()) != 1 || len(b.requests()) != 0 || !reflect.DeepEqual(events(body), events(wire)) {
				t.Fatalf("successful search changed: status=%d a=%d b=%d body=%s", code, len(a.requests()), len(b.requests()), body)
			}
			searchFallbackCleanRequest(t, a.requests()[0], searchFallbackAsk)
			r := searchFallbackRoute(t, s, 1)
			if r.Status != 200 || r.Error != "" || r.Tries[0].Fail != "" || r.Tries[0].Rest != nil || r.Tokens != 18 || r.Output != 5 {
				t.Errorf("successful search trace or usage: %+v", r)
			}
		})
	}
}

func TestResponsesHostedSearchAfterClientContentDoesNotFailOver(t *testing.T) {
	for _, c := range []struct {
		name string
		tool string
		ev   string
		mark string
	}{
		{"text", "", `{"type":"response.output_text.delta","output_index":0,"item_id":"msg_search_a","content_index":0,"delta":"partial answer from a"}`, "partial answer from a"},
		{"commentary", "", `{"type":"response.output_text.delta","output_index":0,"item_id":"msg_search_a","content_index":0,"channel":"commentary","delta":"checking from a"}`, "checking from a"},
		{"function", `{"type":"function","name":"shell","parameters":{"type":"object"}}`, `{"type":"response.output_item.added","output_index":0,"item":{"id":"fc_client_a","type":"function_call","call_id":"call_client_a","name":"shell","arguments":""}}`, "call_client_a"},
		{"custom", `{"type":"custom","name":"shell"}`, `{"type":"response.output_item.added","output_index":0,"item":{"id":"ct_client_a","type":"custom_tool_call","call_id":"call_custom_a","name":"shell","input":""}}`, "call_custom_a"},
	} {
		for _, order := range []string{"before_search", "after_search"} {
			t.Run(c.name+"/"+order, func(t *testing.T) {
				fresh(t)
				ask := searchFallbackAsk
				if c.tool != "" {
					ask = strings.Replace(ask, `"tools":[`, `"tools":[`+c.tool+`,`, 1)
				}
				search := []string{searchFallbackAdded, searchFallbackSearching}
				// The text or client tool commits A regardless of whether
				// hosted search has already begun.
				data := []string{searchFallbackCreated}
				if order == "before_search" {
					data = append(data, c.ev)
					for i := range search {
						search[i] = strings.ReplaceAll(search[i], `"output_index":0`, `"output_index":1`)
					}
				}
				data = append(data, search...)
				if order == "after_search" {
					data = append(data, strings.ReplaceAll(c.ev, `"output_index":0`, `"output_index":1`))
				}
				data = append(data, searchFallbackFailed)
				a := searchFallbackOn(t, "a", searchFallbackSSE(data...))
				b := searchFallbackOn(t, "b", searchFallbackAnswer())
				searchFallbackGroup(t)
				s := New()
				code, body := sendTo(s, "/v1/responses", ask)
				if code != 200 || len(a.requests()) != 1 || len(b.requests()) != 0 || !strings.Contains(body, c.mark) || !strings.Contains(body, "server_is_overloaded") || strings.Contains(body, "answer from b") {
					t.Fatalf("committed reply was replaced: status=%d a=%d b=%d body=%s", code, len(a.requests()), len(b.requests()), body)
				}
				searchFallbackCleanRequest(t, a.requests()[0], ask)
				r := searchFallbackRoute(t, s, 1)
				if r.Status != 200 || r.Tries[0].Status != 200 || r.Tries[0].Fail != failOther || !strings.Contains(r.Error, "overloaded") || !strings.Contains(r.Tries[0].Error, "overloaded") {
					t.Errorf("committed reply's failure disappeared from the trace: %+v", r)
				}
			})
		}
	}
}

func TestResponsesHostedSearchBadRequestDoesNotFailOver(t *testing.T) {
	fresh(t)
	invalid := `{"type":"response.failed","response":{"id":"resp_search_a","status":"failed","error":{"type":"invalid_request_error","code":"invalid_argument","message":"invalid request: input is malformed"}}}`
	wire := searchFallbackSSE(searchFallbackCreated, searchFallbackAdded, searchFallbackSearching, searchFallbackCompleted, searchFallbackDone, invalid)
	a := searchFallbackOn(t, "a", wire)
	b := searchFallbackOn(t, "b", searchFallbackAnswer())
	searchFallbackGroup(t)
	s := New()
	code, body := sendTo(s, "/v1/responses", searchFallbackAsk)
	if code != http.StatusOK || len(a.requests()) != 1 || len(b.requests()) != 0 {
		t.Fatalf("invalid request retried or changed HTTP status: status=%d a=%d b=%d body=%s", code, len(a.requests()), len(b.requests()), body)
	}
	if !reflect.DeepEqual(events(body), events(wire)) {
		t.Errorf("final failed search response changed or lost its IDs/sources: %s", body)
	}
	searchFallbackCleanRequest(t, a.requests()[0], searchFallbackAsk)
	r := searchFallbackRoute(t, s, 1)
	if r.Status != 400 || r.Tries[0].ID != "a" || r.Tries[0].Status != 400 || r.Tries[0].Rest != nil || !strings.Contains(r.Error, "invalid request: input is malformed") || !strings.Contains(r.Tries[0].Error, "invalid request: input is malformed") {
		t.Errorf("invalid request's trace: %+v", r)
	}
}

func TestResponsesProtocolFramesStillFailOver(t *testing.T) {
	fresh(t)
	a := searchFallbackOn(t, "a", searchFallbackSSE(
		searchFallbackCreated,
		`{"type":"response.in_progress","response":{"id":"resp_search_a","status":"in_progress","output":[]}}`,
		`{"type":"response.rate_limits.updated","rate_limits":{"allowed":true}}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"id":"cs_search_a","type":"compaction_summary"}}`,
		`{"type":"keepalive"}`,
		searchFallbackFailed,
	))
	b := searchFallbackOn(t, "b", searchFallbackAnswer())
	searchFallbackGroup(t)
	s := New()
	code, body := sendTo(s, "/v1/responses", searchFallbackAsk)
	searchFallbackHandedOver(t, s, code, body, a, b)
	for _, leaked := range []string{"cs_search_a", "keepalive", "response.rate_limits.updated"} {
		if strings.Contains(body, leaked) {
			t.Errorf("failed protocol frame %q leaked: %s", leaked, body)
		}
	}
}

// The Search API's results are encoded as hosted search too. A follow-up
// failure must discard those results before another group member answers.
func TestResponsesSearchAPIFollowupFailsOver(t *testing.T) {
	fresh(t)
	cfg := settings.Load()
	cfg.Searcher = "off" // use the local Search API, not B as a searcher
	if err := settings.Save(cfg); err != nil {
		t.Fatal(err)
	}
	apis := newSearchAPIs(t)
	if err := provider.SetSearchAPI(apis.api("tavily", "tvly-k")); err != nil {
		t.Fatal(err)
	}
	firstRound := searchFallbackSSE(
		searchFallbackCreated,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_search_a","call_id":"call_search_a","name":"web_search","arguments":""}}`,
		`{"type":"response.function_call_arguments.delta","output_index":0,"item_id":"fc_search_a","delta":"{\"query\":\"query from a\"}"}`,
		`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_search_a","call_id":"call_search_a","name":"web_search","arguments":"{\"query\":\"query from a\"}"}}`,
		`{"type":"response.completed","response":{"id":"resp_search_a","model":"m","status":"completed","output":[{"type":"function_call","id":"fc_search_a","call_id":"call_search_a","name":"web_search","arguments":"{\"query\":\"query from a\"}"}],"usage":{"input_tokens":30,"output_tokens":7}}}`,
	)
	a := &searchFallbackUpstream{rounds: []reply{
		{200, "text/event-stream", firstRound},
		{529, "application/json", `{"error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`},
	}}
	searchFallbackServe(t, "a", a, false)
	b := searchFallbackOn(t, "b", searchFallbackAnswer())
	searchFallbackGroup(t)
	s := New()
	code, body := sendTo(s, "/v1/responses", searchFallbackAsk)
	aRequests, bRequests := a.requests(), b.requests()
	if code != 200 || len(aRequests) != 2 || len(bRequests) != 1 || !strings.Contains(body, "answer from b") {
		t.Fatalf("Search API follow-up did not fail over: status=%d a=%d b=%d body=%s", code, len(aRequests), len(bRequests), body)
	}
	apis.mu.Lock()
	asked := append([]string(nil), apis.asked...)
	apis.mu.Unlock()
	if !reflect.DeepEqual(asked, []string{"tavily: query from a"}) || !strings.Contains(aRequests[1], "https://go.dev/dl/") || !strings.Contains(aRequests[1], "function_call_output") {
		t.Errorf("fixture did not complete the search and follow-up: API=%v requests=%v", asked, aRequests)
	}
	for _, leaked := range []string{"resp_search_a", "fc_search_a", "call_search_a", "web_search_call", "query from a", "https://go.dev/dl/", "server_is_overloaded", "response.failed"} {
		if strings.Contains(body, leaked) {
			t.Errorf("failed Search API result %q leaked to the client: %s", leaked, body)
		}
	}
	searchFallbackCleanRequest(t, bRequests[0], searchFallbackAsk)
	r := searchFallbackRoute(t, s, 2)
	if r.Status != 200 || r.Error != "" || r.Tries[0].Status != 529 || r.Tries[0].Fail != failOther || r.Tries[0].Rest == nil || r.Tries[1].Status != 200 || r.Tries[1].Error != "" {
		t.Errorf("Search API handoff trace: %+v", r)
	}
}

func TestResponsesHostedSearchCodexEntryFallsOver(t *testing.T) {
	fresh(t)
	a := searchFallbackOn(t, "a", searchFallbackSSE(
		searchFallbackCreated, searchFallbackAdded, searchFallbackInProgress,
		searchFallbackSearching, searchFallbackCompleted, searchFallbackDone, searchFallbackCapacity,
	))
	b := searchFallbackOn(t, "b", searchFallbackAnswer())
	searchFallbackGroup(t)
	s := New()
	rec := codexOn(s, CodexPath+"/responses", searchFallbackAsk)
	body := rec.Body.String()
	aRequests, bRequests := a.requests(), b.requests()
	if rec.Code != http.StatusOK || len(aRequests) != 1 || len(bRequests) != 1 || !strings.Contains(body, "answer from b") {
		t.Fatalf("Codex entry did not fail over: status=%d a=%d b=%d body=%s", rec.Code, len(aRequests), len(bRequests), body)
	}
	for _, leaked := range []string{"resp_search_a", "ws_search_a", "query from a", "source-a.invalid", "server_is_overloaded", "Selected model is at capacity", "response.failed"} {
		if strings.Contains(body, leaked) || strings.Contains(bRequests[0], leaked) {
			t.Errorf("Codex entry leaked failed search %q: body=%s next request=%s", leaked, body, bRequests[0])
		}
	}
	// codexInput may normalize the input; both candidates must still get
	// the user's question and hosted search, not the failed search history.
	for _, request := range []string{aRequests[0], bRequests[0]} {
		var got struct {
			Model string `json:"model"`
			Tools []struct {
				Type string `json:"type"`
			} `json:"tools"`
		}
		if err := json.Unmarshal([]byte(request), &got); err != nil {
			t.Fatal(err)
		}
		if got.Model != "m" || len(got.Tools) != 1 || got.Tools[0].Type != "web_search" || !strings.Contains(request, "Find the search fallback fixture.") {
			t.Errorf("Codex entry lost the original question or search tool: %s", request)
		}
	}
	r := searchFallbackRoute(t, s, 2)
	first, last := r.Tries[0], r.Tries[1]
	if r.Status != 200 || r.Error != "" || first.ID != "a" || first.Status != 529 || first.Fail != failOther || first.Rest == nil || first.TTFT != 0 || last.ID != "b" || last.Status != 200 || last.Error != "" || last.Fail != "" {
		t.Errorf("Codex entry handoff trace: %+v", r)
	}
}

func TestResponsesHostedSearchAfter5xxFallsOver(t *testing.T) {
	fresh(t)
	a := &searchFallbackUpstream{rounds: []reply{
		{502, "application/json", `{"error":{"message":"Upstream HTTP/2 stream failed"}}`},
	}}
	searchFallbackServe(t, "a", a, true)
	search := strings.NewReplacer(
		"resp_search_a", "resp_search_b", "ws_search_a", "ws_search_b",
		"query from a", "query from b", "source-a.invalid", "source-b.invalid",
	).Replace(searchFallbackSSE(
		searchFallbackCreated, searchFallbackAdded, searchFallbackSearching,
		searchFallbackCompleted, searchFallbackDone, searchFallbackCapacity,
	))
	b := searchFallbackOn(t, "b", search)
	answer := strings.NewReplacer("resp_answer_b", "resp_answer_c", "msg_answer_b", "msg_answer_c", "answer from b", "answer from c").Replace(searchFallbackAnswer())
	c := searchFallbackOn(t, "c", answer)
	if err := provider.SaveGroup(provider.Group{
		Name: "G", Members: []string{"a/m", "b/m", "c/m"}, Routing: provider.Ordered, Patience: provider.PatienceOff,
	}); err != nil {
		t.Fatal(err)
	}
	s := New()
	code, body := sendTo(s, "/v1/responses", searchFallbackAsk)
	aRequests, bRequests, cRequests := a.requests(), b.requests(), c.requests()
	if code != http.StatusOK || len(aRequests) != 1 || len(bRequests) != 1 || len(cRequests) != 1 || !strings.Contains(body, "answer from c") {
		t.Fatalf("5xx then search capacity did not reach C: status=%d a=%d b=%d c=%d body=%s", code, len(aRequests), len(bRequests), len(cRequests), body)
	}
	for _, leaked := range []string{"Upstream HTTP/2 stream failed", "resp_search_b", "ws_search_b", "query from b", "source-b.invalid", "server_is_overloaded", "Selected model is at capacity", "response.failed"} {
		if strings.Contains(body, leaked) {
			t.Errorf("earlier failure %q leaked to the client: %s", leaked, body)
		}
	}
	for _, request := range []string{aRequests[0], bRequests[0], cRequests[0]} {
		searchFallbackCleanRequest(t, request, searchFallbackAsk)
	}
	r := searchFallbackRoute(t, s, 3)
	first, second, last := r.Tries[0], r.Tries[1], r.Tries[2]
	if r.Status != 200 || r.Error != "" || first.ID != "a" || first.Status != 502 || first.Fail != failOther || first.Rest == nil || second.ID != "b" || second.Status != 529 || second.Fail != failOther || second.Rest == nil || second.TTFT != 0 || last.ID != "c" || last.Status != 200 || last.Error != "" || last.Fail != "" {
		t.Errorf("5xx then search capacity handoff trace: %+v", r)
	}
}
