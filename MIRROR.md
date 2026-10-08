# ccxt Go v4 mirror for AlphaFox

This repository holds the `go/v4` module of [ccxt](https://github.com/ccxt/ccxt) (MIT),
taken from `github.com/ccxt/ccxt/go/v4` at release **v4.5.77**, with these changes:

- `gate.go` signs and sends the same query key order.
- Hyperliquid initialization uses the non-generated `hyperliquid_init.go` helper,
  with narrow call-site changes in `hyperliquid.go`:
  - `InitializeClient` shares one in-flight initialization per native core through
    a type-checked private `Options.LoadOrStore` state. Concurrent callers receive
    separate buffered result channels; there is no goroutine per waiter. After a
    completed best-effort run, calls with no work due return an already populated,
    closed buffered channel without starting another task. The state has no caller
    context or cancellation dependency. It is not an engine/global/account singleton.
  - Readiness is evaluated against current options, not a permanent `sync.Once`.
    `builderFeeAutoApprove` defaults to true; false skips automatic approval. Actual
    approval still follows the upstream builder/fee options (`builderFee=false`
    approves a zero fee, rather than pretending approval succeeded). A recognized
    API failure disables the fee and retries only after `builderFeeApprovalRetryDelay`
    ms (default 3600000) from `builderFeeApprovalFailedAt`. Actual successful approval
    requires a `status: ok` response, sets `approvedBuilderFee`, and clears the failure
    mark; malformed/false responses enter backoff instead. Initialization does not
    preset `refSet`; the existing attempted-once `SetRef` behavior remains.
  - A failed, non-string, or unknown-string `userAbstraction` lookup stays unknown
    (`nil`), with `enableUnifiedMarginRetryDelay` ms (default 300000) after
    `enableUnifiedMarginFailedAt` before retry. Recognized modes are `unifiedAccount`,
    `portfolioMargin`, `disabled`, `default`, and `dexAbstraction`; only the first is
    unified. A recognized successful lookup clears the failure mark. Initialization
    dynamically resolves `fetchBalance` method options, global options, and their
    `defaultEnableUnifiedMargin` aliases using the existing option resolver. Both
    preset true and preset false skip lookup; removing a preset permits lookup again
    once any existing backoff expires. Explicit `IsUnifiedEnabled(..., true)` refresh
    still bypasses its lookup cache/backoff; direct helper calls are not singleflight.
  - Init subtasks run serially instead of `promiseAll`, reducing overlapping signed
    init requests. This does **not** guarantee distinct millisecond nonces, serialize
    orders, or coordinate nonce allocation across clients/processes/accounts.
  - `true` means the current best-effort initialization completed; recognized API
    failures can still have pending delayed retries. False/nil are not cached as
    success and native private-call sites reject them. Unknown panics are not
    silently downgraded to false or ordinary retry failures: waiters are released
    with the CCXT `panic:` string-channel error protocol, prefixed with
    `panic:alphafox_hyperliquid_init:` for managed consumers to identify fatal runtime
    failures. Unknown panic results (including readiness failures) are sticky for
    that native core: subsequent init calls immediately return the same error rather
    than retrying a potentially corrupted client. Recognized errors that must
    propagate (such as invalid proxy settings) retain the ordinary CCXT error
    encoding without this fatal marker and do not become sticky runtime failures.
    `CreateReturnError` checks the marker before stripping panic prefixes or parsing
    nested CCXT exceptions, and returns `*HyperliquidInitializationRuntimeError`.
    This preserves the original text and implements `FatalRuntime() bool` returning
    true across all generated typed wrappers, including cancellation methods.
  - Response-body failures are classified where they occur in `exchange_req.go`:
    plain body read errors become `NetworkError`; gzip header/decompression errors
    become `BadResponse` (invalid or incomplete encoded response). These I/O failures
    therefore retain initialization backoff instead of poisoning the native core as
    unknown runtime panics. Arbitrary panic strings are still never broadly ignored.
  - `HyperLiquidClientInitializationVersion() int` returns **1** on the native core
    and is promoted to the typed wrapper. Managed-init consumers must feature-detect
    this exact method and reject unsupported forks before using their managed path;
    the previous `v4.5.77` fork does not provide this contract. Publish/pin a new fork
    version before enabling that consumer path; the historical replace below is not
    a claim that the old tag includes this work.
  - `CreateOrder` / `CreateOrders` accept an opt-in local-only `alphafoxInitTiming` parameter of type
    `func(int64)`. It is removed before order request construction and called with the
    monotonic elapsed milliseconds waiting on that call's native initialization,
    including expired retries, on both success and failure. It excludes `LoadMarkets`
    and order signing/transport. Callbacks should be short and non-panicking; no hook
    is stored in global/per-core options. Subtracting this from the create-call time
    is not a pure exchange HTTP latency measurement.
  - `hyperliquid_init_test.go` supplies in-memory HTTP transport and coordination
    tests for singleflight, immediate hot returns, delayed retry/recovery, dynamic
    presets/method overrides, unknown modes, opt-out/re-enable of approval, error and
    panic waiter release, false/nil rejection, state validation, timing-hook stripping,
    fatal conversion through nested exceptions and a real cancel wrapper, and plain/
    gzip response-read failures entering builder/unified backoff. A complete typed
    `CreateOrder` test uses local market fixtures and native order signing, asserts
    timing callback execution exactly once, and checks every serialized HTTP body
    for hook leakage. These tests were added but **not run**; no ccxt build,
    Go test, or compilation was performed.

Why a separate repository: resolving that module from `ccxt/ccxt` (or from a fork of
it) makes the Go toolchain clone every ref of the source repository — about 1.06M refs
and 8.4 GB — which does not fit in a CI job. This repository carries only the module
source (about 35 MB, one commit), so the fetch takes seconds.

The module sits at the repository root and keeps the upstream declared path
`github.com/ccxt/ccxt/go/v4`, because 106 files inside the module import that path.
The repository path carries the `/v4` version suffix, so Go resolves the tag `v4.5.77`.
The module is only usable through a `replace`, which supplies the import paths used by
the consumer.

Consumers pin it with:

    replace github.com/ccxt/ccxt/go/v4 => github.com/alphafoxai/ccxt-go/v4 v4.5.77

The upstream report for the Gate fix is https://github.com/ccxt/ccxt/issues/30561 and
the proposed upstream patch is https://github.com/ccxt/ccxt/pull/30562. Delete this
repository and the `replace` directives once the fix ships upstream.
