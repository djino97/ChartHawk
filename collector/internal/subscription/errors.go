package subscription

import "fmt"

// ErrKind классифицирует ошибку Apply для маппинга в HTTP-код ответа.
type ErrKind int

const (
	// ErrKindValidation — невалидный запрос (пустой список, неизвестный
	// таймфрейм, дубликаты) → HTTP 400.
	ErrKindValidation ErrKind = iota
	// ErrKindUnknownTicker — тикер не найден в справочнике → HTTP 422.
	ErrKindUnknownTicker
	// ErrKindLimitExceeded — превышен лимит подписок → HTTP 409.
	ErrKindLimitExceeded
	// ErrKindApply — ошибка при применении подписок на стороне T-Invest API
	// (после успешной валидации) → HTTP 500.
	ErrKindApply
)

// Error — типизированная ошибка применения подписок.
type Error struct {
	Kind    ErrKind
	Message string
	Ticker  string // заполняется для ErrKindUnknownTicker
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }

func newValidationErr(format string, args ...any) *Error {
	return &Error{Kind: ErrKindValidation, Message: fmt.Sprintf(format, args...)}
}

func newUnknownTickerErr(ticker string) *Error {
	return &Error{
		Kind:    ErrKindUnknownTicker,
		Message: fmt.Sprintf("ticker %q not found in instrument reference", ticker),
		Ticker:  ticker,
	}
}

func newLimitExceededErr(requested, limit int) *Error {
	return &Error{
		Kind:    ErrKindLimitExceeded,
		Message: fmt.Sprintf("requested %d instruments exceeds limit of %d", requested, limit),
	}
}

func newApplyErr(err error) *Error {
	return &Error{Kind: ErrKindApply, Message: "failed to apply subscriptions", Err: err}
}
