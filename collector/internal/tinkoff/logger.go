package tinkoff

import (
	"fmt"
	"log/slog"
)

// sdkLogger адаптирует стандартный slog.Logger к интерфейсу
// investgo.Logger, требуемому официальным SDK.
type sdkLogger struct {
	l *slog.Logger
}

func newSDKLogger(l *slog.Logger) *sdkLogger {
	return &sdkLogger{l: l}
}

func (s *sdkLogger) Infof(template string, args ...any) {
	s.l.Info(fmt.Sprintf(template, args...))
}

func (s *sdkLogger) Errorf(template string, args ...any) {
	s.l.Error(fmt.Sprintf(template, args...))
}

func (s *sdkLogger) Fatalf(template string, args ...any) {
	s.l.Error(fmt.Sprintf(template, args...))
}
