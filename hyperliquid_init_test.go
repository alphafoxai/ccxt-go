package ccxt

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func awaitHyperliquidInitialization(t *testing.T, ch <-chan any) any {
	t.Helper()
	select {
	case result, ok := <-ch:
		if !ok {
			t.Fatal("initialization channel closed without a result")
		}
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("initialization waiter was not released")
		return nil
	}
}

func TestHyperliquidInitializationSingleflightAndSynchronousHotPath(t *testing.T) {
	state := &hyperliquidInitializationState{}
	var runs atomic.Int64
	release := make(chan struct{})
	run := func() any { runs.Add(1); <-release; return true }
	const callers = 32
	channels := make([]chan any, callers)
	var wg sync.WaitGroup
	for i := range channels {
		channels[i] = make(chan any, 1)
		wg.Add(1)
		go func(ch chan any) {
			defer wg.Done()
			state.initialize(ch, func() bool { return true }, run)
		}(channels[i])
	}
	wg.Wait()
	close(release)
	for _, ch := range channels {
		if got := awaitHyperliquidInitialization(t, ch); got != true {
			t.Fatalf("shared result = %v", got)
		}
	}
	for i := 0; i < 100; i++ {
		ch := make(chan any, 1)
		state.initialize(ch, func() bool { return true }, run)
		// Must be populated before initialize returns, not by a new task.
		if len(ch) != 1 || cap(ch) != 1 || <-ch != true {
			t.Fatal("hot path did not return a buffered immediate success")
		}
	}
	if runs.Load() != 1 {
		t.Fatalf("run count = %d, want 1", runs.Load())
	}
}

func TestHyperliquidInitializationPanicAndFalseRelease(t *testing.T) {
	for _, name := range []string{"panic", "false", "nil"} {
		t.Run(name, func(t *testing.T) {
			state := &hyperliquidInitializationState{}
			release := make(chan struct{})
			run := func() any {
				<-release
				switch name {
				case "panic":
					panic("unknown init bug")
				case "false":
					return false
				default:
					return nil
				}
			}
			channels := make([]chan any, 8)
			for i := range channels {
				channels[i] = make(chan any, 1)
				state.initialize(channels[i], func() bool { return true }, run)
			}
			close(release)
			for _, ch := range channels {
				got := awaitHyperliquidInitialization(t, ch)
				if name == "panic" {
					if !IsError(got) || !strings.HasPrefix(got.(string), "panic:alphafox_hyperliquid_init:") {
						t.Fatalf("lost fatal panic: %v", got)
					}
				} else if name == "false" && got != false || name == "nil" && got != nil {
					t.Fatalf("lost unsuccessful result: %v", got)
				}
			}
			ch := make(chan any, 1)
			state.initialize(ch, func() bool { return true }, func() any { return true })
			got := awaitHyperliquidInitialization(t, ch)
			if name == "panic" {
				if !IsError(got) || !strings.HasPrefix(got.(string), "panic:alphafox_hyperliquid_init:") {
					t.Fatalf("fatal panic was not sticky: %v", got)
				}
			} else if got != true {
				t.Fatalf("false/nil prevented next flight: %v", got)
			}
		})
	}
}

type hyperliquidInitTransport func(*http.Request) (*http.Response, error)

func (f hyperliquidInitTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// All HTTP is intercepted in-memory, including signed requests. No public service
// or listener is used and no production credentials are involved.
func newHyperliquidInitTestCore(t *testing.T, reply func(string) string) *HyperliquidCore {
	t.Helper()
	client := NewHyperliquid(map[string]any{
		"walletAddress":   "0x0000000000000000000000000000000000000001",
		"privateKey":      strings.Repeat("0", 63) + "1",
		"enableRateLimit": false,
	})
	core := client.Core
	core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			return nil, fmt.Errorf("decode test request: %w", err)
		}
		kind, _ := request["type"].(string)
		if action, ok := request["action"].(map[string]any); ok {
			kind, _ = action["type"].(string)
		}
		body := reply(kind)
		return &http.Response{StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	if client.HyperLiquidClientInitializationVersion() != 1 {
		t.Fatal("wrapper does not expose initialization capability")
	}
	return core
}

func TestHyperliquidInitializationNativeCoreSingleflight(t *testing.T) {
	var requests atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	core := newHyperliquidInitTestCore(t, func(kind string) string {
		if requests.Add(1) == 1 {
			close(entered)
			<-release
		}
		if kind == "userAbstraction" {
			return `"disabled"`
		}
		return `{"status":"ok","response":{"type":"default"}}`
	})
	channels := []<-chan any{core.InitializeClient()}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("native init did not reach transport")
	}
	for i := 0; i < 31; i++ {
		channels = append(channels, core.InitializeClient())
	}
	close(release)
	for _, ch := range channels {
		if got := awaitHyperliquidInitialization(t, ch); got != true {
			t.Fatalf("native shared init failed: %v", got)
		}
	}
	if requests.Load() != 3 {
		t.Fatalf("native subtasks duplicated: %d requests, want 3", requests.Load())
	}
	if ch := core.InitializeClient(); len(ch) != 1 || <-ch != true || requests.Load() != 3 {
		t.Fatal("native completed hot path repeated work")
	}
}

func TestHyperliquidInitializationFailureBackoffAndExpiry(t *testing.T) {
	var builder, ref, unified atomic.Int64
	core := newHyperliquidInitTestCore(t, func(kind string) string {
		switch kind {
		case "approveBuilderFee":
			if builder.Add(1) == 1 {
				return `{"status":"err","response":"approval rejected"}`
			}
		case "setReferrer":
			ref.Add(1)
		case "userAbstraction":
			if unified.Add(1) == 1 {
				return `{"status":"err","response":"lookup unavailable"}`
			}
			return `"unifiedAccount"`
		}
		return `{"status":"ok","response":{"type":"default"}}`
	})
	if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true {
		t.Fatalf("known failures must remain best effort: %v", got)
	}
	if IsTrue(core.SafeBool(core.Options, "approvedBuilderFee", false)) || core.SafeValue(core.Options, "enableUnifiedMargin") != nil {
		t.Fatal("failed init fabricated approval or known unified mode")
	}
	for i := 0; i < 10; i++ {
		ch := core.InitializeClient()
		if len(ch) != 1 || <-ch != true {
			t.Fatal("backoff hot path was not immediate")
		}
	}
	if builder.Load() != 1 || ref.Load() != 1 || unified.Load() != 1 {
		t.Fatalf("repeated work during backoff: %d/%d/%d", builder.Load(), ref.Load(), unified.Load())
	}
	// Move failure times beyond the defaults, without sleeps or wall-clock races.
	core.Options.Store("builderFeeApprovalFailedAt", core.Milliseconds()-3600001)
	core.Options.Store("enableUnifiedMarginFailedAt", core.Milliseconds()-300001)
	if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true {
		t.Fatalf("expired retry failed: %v", got)
	}
	if builder.Load() != 2 || unified.Load() != 2 || ref.Load() != 1 {
		t.Fatalf("wrong retry counts: %d/%d/%d", builder.Load(), ref.Load(), unified.Load())
	}
	if !IsTrue(core.SafeBool(core.Options, "approvedBuilderFee", false)) || core.SafeValue(core.Options, "enableUnifiedMargin") != true {
		t.Fatal("recovery did not cache real approval/mode")
	}
	if core.SafeValue(core.Options, "builderFeeApprovalFailedAt") != nil || core.SafeValue(core.Options, "enableUnifiedMarginFailedAt") != nil {
		t.Fatal("successful retry did not clear failure marks")
	}
}

func TestHyperliquidInitializationDynamicPresetAndUnknownMode(t *testing.T) {
	var info, approval atomic.Int64
	core := newHyperliquidInitTestCore(t, func(kind string) string {
		if kind == "userAbstraction" {
			if info.Add(1) == 1 {
				return `"futureUnknownMode"`
			}
			return `"disabled"`
		}
		if kind == "approveBuilderFee" {
			approval.Add(1)
		}
		return `{"status":"ok","response":{"type":"default"}}`
	})
	core.Options.Store("builderFeeAutoApprove", false)
	core.Options.Store("fetchBalance", map[string]any{"enableUnifiedMargin": false})
	if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true || info.Load() != 0 || approval.Load() != 0 {
		t.Fatalf("preset/auto-approve opt-out ignored: %v", got)
	}
	core.Options.Delete("fetchBalance")
	if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true || info.Load() != 1 {
		t.Fatalf("removing dynamic preset did not trigger lookup: %v", got)
	}
	if core.SafeValue(core.Options, "enableUnifiedMargin") != nil {
		t.Fatal("unknown mode was cached as known false")
	}
	core.Options.Store("enableUnifiedMarginFailedAt", core.Milliseconds()-300001)
	// Method/default override must beat retry expiry and avoid lookup.
	core.Options.Store("fetchBalance", map[string]any{"defaultEnableUnifiedMargin": true})
	if ch := core.InitializeClient(); len(ch) != 1 || <-ch != true || info.Load() != 1 {
		t.Fatal("method default override was not respected by hot path")
	}
	core.Options.Delete("fetchBalance")
	if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true || info.Load() != 2 {
		t.Fatalf("unknown mode did not retry after expiry: %v", got)
	}
	if core.SafeValue(core.Options, "enableUnifiedMargin") != false {
		t.Fatal("known disabled mode was not cached")
	}
	core.Options.Store("builderFeeAutoApprove", true)
	if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true || approval.Load() != 1 {
		t.Fatalf("dynamic auto-approve enable did not perform actual approval: %v", got)
	}
}

func TestHyperliquidInitializationStateValidationAndTiming(t *testing.T) {
	core := newHyperliquidInitTestCore(t, func(string) string { panic("unexpected HTTP") })
	core.Options.Store(hyperliquidInitializationStateKey, "invalid")
	got := awaitHyperliquidInitialization(t, core.InitializeClient())
	if !IsError(got) || !strings.HasPrefix(got.(string), "panic:alphafox_hyperliquid_init:") {
		t.Fatalf("invalid state was not reported as fatal: %v", got)
	}
	state := &hyperliquidInitializationState{completed: true}
	core.Options.Store(hyperliquidInitializationStateKey, state)
	core.Options.Store("refSet", true) // only this isolated no-I/O helper test presets ref
	core.Options.Store("builderFeeAutoApprove", false)
	core.Options.Store("enableUnifiedMargin", false)
	called := false
	params := map[string]any{"keep": "value", "alphafoxInitTiming": func(ms int64) {
		called = true
		if ms < 0 {
			t.Error("negative monotonic elapsed time")
		}
	}}
	clean := core.initializeClientForOrders(params)
	if !called || core.SafeValue(clean, "alphafoxInitTiming") != nil || core.SafeValue(clean, "keep") != "value" {
		t.Fatal("timing hook was not invoked and stripped from outgoing params")
	}
	// Readiness panic must produce a buffered fatal result and release the lock.
	ch := make(chan any, 1)
	state.initialize(ch, func() bool { panic("readiness bug") }, func() any { return true })
	if len(ch) != 1 {
		t.Fatal("readiness panic did not return a buffered error")
	}
	failure := <-ch
	if !IsError(failure) || !strings.HasPrefix(failure.(string), "panic:alphafox_hyperliquid_init:") {
		t.Fatalf("readiness panic lost fatal marker: %v", failure)
	}
	ch = make(chan any, 1)
	state.initialize(ch, func() bool { return true }, func() any { return true })
	if len(ch) != 1 || <-ch != failure {
		t.Fatal("readiness fatal was not sticky or stranded mutex")
	}
}

func TestHyperliquidInitializationUnknownPanicIsNotBestEffort(t *testing.T) {
	core := newHyperliquidInitTestCore(t, func(string) string { panic("transport implementation bug") })
	got := awaitHyperliquidInitialization(t, core.InitializeClient())
	if !IsError(got) || !strings.HasPrefix(got.(string), "panic:alphafox_hyperliquid_init:") || !strings.Contains(got.(string), "transport implementation bug") {
		t.Fatalf("unknown panic was swallowed: %v", got)
	}
	if core.SafeValue(core.Options, "builderFeeApprovalFailedAt") != nil {
		t.Fatal("unknown panic incorrectly became a retryable approval failure")
	}
	if ch := core.InitializeClient(); len(ch) != 1 || <-ch != got {
		t.Fatal("native fatal result was not sticky and immediately buffered")
	}
	called := false
	func() {
		defer func() {
			if recover() == nil {
				t.Error("order init did not propagate fatal result")
			}
		}()
		core.initializeClientForOrders(map[string]any{"alphafoxInitTiming": func(int64) { called = true }})
	}()
	if !called {
		t.Fatal("timing hook was not called on initialization failure")
	}
}

func TestHyperliquidInitializationMalformedApprovalIsNotApproved(t *testing.T) {
	core := newHyperliquidInitTestCore(t, func(kind string) string {
		if kind == "approveBuilderFee" {
			return `false`
		}
		if kind == "userAbstraction" {
			return `"disabled"`
		}
		return `{"status":"ok","response":{"type":"default"}}`
	})
	if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true {
		t.Fatalf("malformed response should use best-effort backoff: %v", got)
	}
	if IsTrue(core.SafeBool(core.Options, "approvedBuilderFee", false)) || core.SafeValue(core.Options, "builderFeeApprovalFailedAt") == nil {
		t.Fatal("malformed response fabricated successful builder approval")
	}
}

func TestHyperliquidInitializationErrorClassification(t *testing.T) {
	for _, failure := range []any{InvalidProxySettings("bad proxy"), PanicMessage(InvalidProxySettings("bad proxy"))} {
		if hyperliquidInitializationCanIgnore(failure) || strings.HasPrefix(hyperliquidInitializationPanicMessage(failure), "panic:alphafox_hyperliquid_init:") {
			t.Fatal("recognized proxy error was swallowed or mislabeled fatal")
		}
	}
	if !hyperliquidInitializationCanIgnore(PanicMessage(NetworkError("temporary"))) {
		t.Fatal("recognized network failure lost best-effort retry")
	}
	if hyperliquidInitializationCanIgnore("unknown bug") || hyperliquidInitializationCanIgnore(NewError("customUnknown", "bug")) {
		t.Fatal("unknown failure was treated as ordinary API error")
	}
}

func TestHyperliquidInitializationFatalTypedConversion(t *testing.T) {
	for _, raw := range []string{
		"panic:alphafox_hyperliquid_init:panic:unknown bug",
		hyperliquidInitializationPanicMessage(Exception("inner exception")),
	} {
		err := CreateReturnError(raw)
		var fatal interface{ FatalRuntime() bool }
		if !errors.As(err, &fatal) || !fatal.FatalRuntime() || err.Error() != raw {
			t.Fatalf("conversion lost original fatal provenance: %T %v", err, err)
		}
	}
	ordinary := CreateReturnError(PanicMessage(NetworkError("temporary")))
	var fatal interface{ FatalRuntime() bool }
	if errors.As(ordinary, &fatal) {
		t.Fatal("ordinary API failure incorrectly became fatal")
	}
}

func TestHyperliquidInitializationFatalSurvivesCancelWrapper(t *testing.T) {
	core := newHyperliquidInitTestCore(t, func(string) string { panic("must not send cancel") })
	core.Markets = &sync.Map{} // skip unrelated market loading at cancel boundary
	original := hyperliquidInitializationPanicMessage(Exception("nested initialization failure"))
	core.Options.Store(hyperliquidInitializationStateKey, &hyperliquidInitializationState{fatal: original})
	client := NewHyperliquidFromCore(core)
	_, err := client.CancelAllOrdersAfter(1000)
	var fatal interface{ FatalRuntime() bool }
	if !errors.As(err, &fatal) || !fatal.FatalRuntime() || !strings.Contains(err.Error(), original) {
		t.Fatalf("cancel wrapper lost sticky fatal initialization error: %T %v", err, err)
	}
}

type hyperliquidBrokenResponseBody struct{ io.Reader }

func (body hyperliquidBrokenResponseBody) Read(p []byte) (int, error) {
	if body.Reader != nil {
		n, err := body.Reader.Read(p)
		if n > 0 || err != io.EOF {
			return n, err
		}
	}
	return 0, io.ErrUnexpectedEOF
}
func (hyperliquidBrokenResponseBody) Close() error { return nil }

func TestHyperliquidInitializationResponseIOFailuresBackoff(t *testing.T) {
	for _, encoding := range []string{"plain", "gzip-header", "gzip-body"} {
		t.Run(encoding, func(t *testing.T) {
			core := newHyperliquidInitTestCore(t, func(string) string { return `{"status":"ok"}` })
			var requests atomic.Int64
			core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				header := http.Header{}
				body := hyperliquidBrokenResponseBody{}
				if encoding != "plain" {
					header.Set("Content-Encoding", "gzip")
				}
				if encoding == "gzip-body" {
					var compressed bytes.Buffer
					writer := gzip.NewWriter(&compressed)
					if _, err := writer.Write([]byte(`{"status":"ok"}`)); err != nil {
						return nil, err
					}
					if err := writer.Close(); err != nil {
						return nil, err
					}
					// Keep a valid gzip header but truncate the trailer, then fail
					// the underlying read to exercise decompression-read failure.
					body.Reader = bytes.NewReader(compressed.Bytes()[:compressed.Len()-8])
				}
				return &http.Response{StatusCode: 200, Status: "200 OK", Header: header, Body: body, Request: r}, nil
			})
			if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true {
				t.Fatalf("response I/O failure incorrectly fatal: %v", got)
			}
			if core.SafeValue(core.Options, "builderFeeApprovalFailedAt") == nil || core.SafeValue(core.Options, "enableUnifiedMarginFailedAt") == nil {
				t.Fatal("approval and unified I/O failures did not both enter backoff")
			}
			if core.hyperliquidInitializationState().fatal != nil {
				t.Fatal("response I/O failure poisoned native core")
			}
			before := requests.Load()
			if ch := core.InitializeClient(); len(ch) != 1 || <-ch != true || requests.Load() != before {
				t.Fatal("response I/O failure was retried inside backoff")
			}
		})
	}
}

func TestHyperliquidCreateOrderTimingHookEndToEnd(t *testing.T) {
	core := newHyperliquidInitTestCore(t, func(string) string { panic("transport replaced below") })
	// No preexisting market fixture exists in this package's tests. Use the native
	// SafeMarketStructure shape emitted by Hyperliquid.ParseMarket, in sync maps.
	const symbol = "BTC/USDC:USDC"
	market := core.SafeMarketStructure(map[string]any{
		"id": "0", "baseId": "0", "baseName": "BTC", "symbol": symbol,
		"base": "BTC", "quote": "USDC", "settle": "USDC",
		"type": "swap", "spot": false, "swap": true, "future": false,
		"option": false, "contract": true, "linear": true, "inverse": false,
		"active": true, "contractSize": 1,
		"precision": map[string]any{"amount": 0.001, "price": 1.0},
	})
	core.Markets = &sync.Map{}
	core.Markets.Store(symbol, market)
	core.Markets_by_id = &sync.Map{}
	core.Markets_by_id.Store("0", []any{market})
	core.MarketsById = core.Markets_by_id
	var callbackCount, orderRequests atomic.Int64
	core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if bytes.Contains(body, []byte("alphafoxInitTiming")) {
			t.Error("local timing hook leaked into signed wire JSON")
		}
		var request map[string]any
		if err := json.Unmarshal(body, &request); err != nil {
			return nil, err
		}
		reply := `{"status":"ok","response":{"type":"default"}}`
		if kind, _ := request["type"].(string); kind == "userAbstraction" {
			reply = `"disabled"`
		} else if action, ok := request["action"].(map[string]any); ok {
			if action["type"] == "order" {
				orderRequests.Add(1)
				if callbackCount.Load() != 1 {
					t.Error("timing callback must run exactly once before order transport")
				}
				signature, ok := request["signature"].(map[string]any)
				if !ok || signature["r"] == nil || signature["s"] == nil || signature["v"] == nil || request["nonce"] == nil {
					t.Error("order did not traverse native signing")
				}
				orders, ok := action["orders"].([]any)
				if !ok || len(orders) != 1 {
					t.Error("expected one serialized order")
				}
				reply = `{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":12345}}]}}}`
			}
		} else {
			t.Errorf("unexpected request (market network loading must be skipped): %s", body)
		}
		return &http.Response{StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(reply)), Request: r}, nil
	})
	client := NewHyperliquidFromCore(core)
	order, err := client.CreateOrder(symbol, "limit", "buy", 0.001,
		WithCreateOrderPrice(50000), WithCreateOrderParams(map[string]any{
			"alphafoxInitTiming": func(ms int64) {
				callbackCount.Add(1)
				if ms < 0 {
					t.Error("negative native init elapsed time")
				}
			},
		}))
	if err != nil {
		t.Fatalf("typed CreateOrder full path failed: %v", err)
	}
	if callbackCount.Load() != 1 || orderRequests.Load() != 1 {
		t.Fatalf("callback/order request counts = %d/%d, want 1/1", callbackCount.Load(), orderRequests.Load())
	}
	if order.Id == nil || *order.Id != "12345" {
		t.Fatalf("typed response did not parse returned order id: %+v", order.Id)
	}
}

func TestHyperliquidInitializationRejectsFalseAndNil(t *testing.T) {
	for _, result := range []any{false, nil} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("initialization result %v allowed private call to continue", result)
				}
			}()
			hyperliquidCheckInitialization(result)
		}()
	}
}
