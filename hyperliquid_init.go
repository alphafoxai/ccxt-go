package ccxt

// AlphaFox fork extension; kept outside generated exchange code for review.
import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const hyperliquidInitializationStateKey = "__alphafoxHyperliquidInitializationV1"

// HyperliquidInitializationRuntimeError preserves fatal initialization provenance
// across generated typed wrapper conversions, including nested CCXT exceptions.
// Consumers can use the structural FatalRuntime interface without importing a guard.
type HyperliquidInitializationRuntimeError struct {
	Message string
}

func (e *HyperliquidInitializationRuntimeError) Error() string      { return e.Message }
func (e *HyperliquidInitializationRuntimeError) FatalRuntime() bool { return true }

// Preserve CCXT's string-channel error protocol, with a stable marker for managed
// consumers to distinguish unknown panics from recognized CCXT API errors.
func hyperliquidInitializationPanicMessage(r any) string {
	message := PanicMessage(r)
	if strings.Contains(message, "panic:alphafox_hyperliquid_init:") || hyperliquidInitializationErrorKind(r) == "" {
		return "panic:alphafox_hyperliquid_init:" + message
	}
	return message
}

type hyperliquidInitializationState struct {
	mu        sync.Mutex
	running   bool
	completed bool
	waiters   []chan any
	fatal     any // unknown runtime failures are sticky for this native core
}

// HyperLiquidClientInitializationVersion lets managed-init consumers reject forks
// that swallow initialization errors. It is promoted to *Hyperliquid as well.
func (this *HyperliquidCore) HyperLiquidClientInitializationVersion() int { return 1 }

func (this *HyperliquidCore) hyperliquidInitializationState() *hyperliquidInitializationState {
	if this.Options == nil {
		panic("hyperliquid initialization requires initialized Options")
	}
	value, ok := this.Options.Load(hyperliquidInitializationStateKey)
	if !ok {
		value, _ = this.Options.LoadOrStore(hyperliquidInitializationStateKey, &hyperliquidInitializationState{})
	}
	state, ok := value.(*hyperliquidInitializationState)
	if !ok || state == nil {
		panic(fmt.Sprintf("hyperliquid initialization state has invalid type %T", value))
	}
	return state
}

func (this *HyperliquidCore) initializeClientShared() (result <-chan any) {
	ch := make(chan any, 1)
	result = ch
	// Preserve the async API even when state/options validation panics synchronously.
	defer func() {
		if r := recover(); r != nil {
			ch <- hyperliquidInitializationPanicMessage(r)
			close(ch)
		}
	}()
	this.hyperliquidInitializationState().initialize(ch, this.hyperliquidInitializationReady, this.initializeClientTasks)
	return ch
}

func (state *hyperliquidInitializationState) initialize(ch chan any, ready func() bool, run func() any) {
	state.mu.Lock()
	defer state.mu.Unlock()
	// Readiness itself can panic (bad dynamic options); release the lock, return
	// an error on the normal channel, and make unknown failures sticky too.
	defer func() {
		if r := recover(); r != nil {
			result := hyperliquidInitializationPanicMessage(r)
			if strings.HasPrefix(result, "panic:alphafox_hyperliquid_init:") {
				state.fatal = result
			}
			ch <- result
			close(ch)
		}
	}()
	if state.fatal != nil {
		ch <- state.fatal
		close(ch)
		return
	}
	if state.running {
		// A separate buffered result per caller: no shared channel consumption and
		// no goroutine per waiter. Abandoned callers cannot block completion.
		state.waiters = append(state.waiters, ch)
		return
	}
	if state.completed && ready() {
		ch <- true
		close(ch)
		return
	}
	state.running = true
	state.waiters = append(state.waiters, ch)
	go state.run(run)
}

func (state *hyperliquidInitializationState) run(run func() any) {
	var result any
	defer func() {
		if r := recover(); r != nil {
			result = hyperliquidInitializationPanicMessage(r)
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		// false, nil and errors never become successful cached initialization.
		completed, ok := result.(bool)
		state.completed = ok && completed
		if message, ok := result.(string); ok && strings.HasPrefix(message, "panic:alphafox_hyperliquid_init:") {
			state.fatal = result
		}
		state.running = false
		for _, ch := range state.waiters {
			ch <- result
			close(ch)
		}
		state.waiters = nil
	}()
	result = run()
}

func (this *HyperliquidCore) hyperliquidInitializationBackoff(key, delayKey string, defaultDelay int64) bool {
	failedAt := this.SafeInteger(this.Options, key)
	if failedAt == nil {
		return false
	}
	delay := this.SafeInteger(this.Options, delayKey, defaultDelay)
	return IsTrue(IsLessThan(Subtract(this.Milliseconds(), failedAt), delay))
}

func (this *HyperliquidCore) hyperliquidInitializationReady() bool {
	if !IsTrue(this.SafeBool(this.Options, "refSet", false)) {
		return false
	}
	if IsTrue(this.SafeBool(this.Options, "builderFeeAutoApprove", true)) &&
		!IsTrue(this.SafeBool(this.Options, "approvedBuilderFee", false)) &&
		!this.hyperliquidInitializationBackoff("builderFeeApprovalFailedAt", "builderFeeApprovalRetryDelay", 3600000) {
		return false
	}
	// Resolve this dynamically, using exactly the same method/global/default
	// precedence as IsUnifiedEnabled. In particular, a preset false is known,
	// and removing a preset must not leave a permanent successful init cache.
	resolved := this.HandleOptionAndParams(map[string]any{}, "fetchBalance", "enableUnifiedMargin")
	return GetValue(resolved, 0) != nil ||
		this.hyperliquidInitializationBackoff("enableUnifiedMarginFailedAt", "enableUnifiedMarginRetryDelay", 300000)
}

func hyperliquidCheckInitialization(result any) {
	PanicOnError(result)
	if success, ok := result.(bool); !ok || !success {
		panic(ExchangeError("hyperliquid InitializeClient did not return true"))
	}
}

// alphafoxInitTiming is an opt-in diagnostic hook, never an exchange parameter.
// It measures only this call's native init wait, including an expired retry.
func (this *HyperliquidCore) initializeClientForOrders(params any) any {
	hook := this.SafeValue(params, "alphafoxInitTiming")
	params = this.Omit(params, "alphafoxInitTiming")
	var timing func(int64)
	if hook != nil {
		var ok bool
		timing, ok = hook.(func(int64))
		if !ok {
			panic(ArgumentsRequired("hyperliquid alphafoxInitTiming must be func(int64)"))
		}
	}
	start := time.Now()
	result := <-this.InitializeClient()
	if timing != nil {
		timing(time.Since(start).Milliseconds())
	}
	hyperliquidCheckInitialization(result)
	return params
}

func (this *HyperliquidCore) initializeClientTasks() any {
	// Serializing these tasks reduces competing signed init requests. This is NOT
	// an account-wide nonce allocator (nor a guarantee of distinct millisecond nonces).
	result := <-this.HandleBuilderFeeApproval()
	PanicOnError(result)
	if value, ok := result.(bool); ok && !value {
		return false
	}
	result = <-this.SetRef()
	PanicOnError(result)
	if value, ok := result.(bool); ok && !value {
		return false
	}
	result = <-this.IsUnifiedEnabled("fetchBalance", nil, false, map[string]any{})
	PanicOnError(result)
	if value, ok := result.(bool); ok && !value {
		return false
	}
	return true // best effort: known API failures retain their existing retry delay
}

// Generated async boundaries encode errors as strings. Only identified CCXT
// errors are eligible for best-effort fallback; runtime/unknown panics must escape.
func hyperliquidInitializationCanIgnore(r any) bool {
	if message, ok := r.(string); ok && strings.Contains(message, "panic:alphafox_hyperliquid_init:") {
		return false
	}
	kind := hyperliquidInitializationErrorKind(r)
	return kind != "" && kind != "InvalidProxySettings"
}

func hyperliquidInitializationErrorKind(r any) string {
	var kind string
	if err, ok := r.(*Error); ok && err != nil {
		kind = string(err.Type)
	} else if message, ok := r.(string); ok {
		message = strings.SplitN(message, "\nStack trace:", 2)[0]
		_, suffix, found := strings.Cut(message, "[ccxtError]::[")
		if found {
			kind, _, _ = strings.Cut(suffix, "]::[")
		}
	}
	// Only known CCXT classes are eligible; Exception and arbitrary custom error
	// names must not disguise an unknown programming/runtime failure as API retry.
	switch ErrorType(kind) {
	case ExchangeErrorErrType, AuthenticationErrorErrType, PermissionDeniedErrType,
		AccountNotEnabledErrType, AccountSuspendedErrType, ArgumentsRequiredErrType,
		BadRequestErrType, BadSymbolErrType, OperationRejectedErrType, NoChangeErrType,
		MarginModeAlreadySetErrType, MarketClosedErrType, ManualInteractionNeededErrType,
		RestrictedLocationErrType, InsufficientFundsErrType, InvalidAddressErrType,
		AddressPendingErrType, InvalidOrderErrType, OrderNotFoundErrType, OrderNotCachedErrType,
		OrderImmediatelyFillableErrType, OrderNotFillableErrType, DuplicateOrderIdErrType,
		ContractUnavailableErrType, NotSupportedErrType, InvalidProxySettingsErrType,
		ExchangeClosedByUserErrType, OperationFailedErrType, NetworkErrorErrType,
		DDoSProtectionErrType, RateLimitExceededErrType, ExchangeNotAvailableErrType,
		OnMaintenanceErrType, InvalidNonceErrType, ChecksumErrorErrType, RequestTimeoutErrType,
		BadResponseErrType, NullResponseErrType, CancelPendingErrType, UnsubscribeErrorErrType:
		return kind
	default:
		return ""
	}
}

func hyperliquidUnifiedMode(response any) (enabled any, known bool) {
	mode, ok := response.(string)
	if !ok {
		return nil, false
	}
	switch strings.Trim(mode, "\"") {
	case "unifiedAccount":
		return true, true
	case "portfolioMargin", "disabled", "default", "dexAbstraction":
		return false, true
	default:
		return nil, false
	}
}
