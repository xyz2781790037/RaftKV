package tool

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"os"
)

type Color string

const (
	Reset   Color = "\033[0m"
	Red     Color = "\033[1;31m"
	Green   Color = "\033[1;32m"
	Yellow  Color = "\033[1;33m"
	Blue    Color = "\033[1;34m"
	Magenta Color = "\033[1;35m"
)

var logLevelColorMap = map[slog.Level]Color{
	slog.LevelDebug: Blue,
	slog.LevelInfo:  Green,
	slog.LevelWarn:  Magenta,
	slog.LevelError: Red,
}

type ColoredHandler struct {
	slog.Handler
	w io.Writer
}

func NewColoredHandler(out io.Writer, opts *slog.HandlerOptions) *ColoredHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}

	// 关闭默认的 Source 字段
	opts.AddSource = false

	return &ColoredHandler{
		Handler: slog.NewTextHandler(out, opts),
		w:       out,
	}
}
func (h *ColoredHandler) Handle(ctx context.Context, r slog.Record) error {
	color, ok := logLevelColorMap[r.Level]
	if !ok {
		color = Reset
	}

	levelStr := fmt.Sprintf("[%s%s%s]", color, r.Level.String(), Reset)

	timeStr := r.Time.Format("2006-01-02 15:04:05.000")

	fs := runtime.CallersFrames([]uintptr{r.PC})
	f, _ := fs.Next()

	source := fmt.Sprintf("%s:%d", filepath.Base(f.File), f.Line)
	sourceStr := fmt.Sprintf("[%s%s%s]", Yellow, source, Reset)

	messageStr := fmt.Sprintf("%s%s%s", color, r.Message, Reset)

	output := fmt.Sprintf("%s %s %s %s", timeStr, levelStr, sourceStr, messageStr)

	r.Attrs(func(a slog.Attr) bool {
		output += fmt.Sprintf(" %s=%v", a.Key, a.Value.Any())
		return true
	})

	output += "\n"

	_, err := h.w.Write([]byte(output))
	return err
}
func (h *ColoredHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ColoredHandler{
		Handler: h.Handler.WithAttrs(attrs),
		w: h.w,
	}
}

func (h *ColoredHandler) WithGroup(name string) slog.Handler {
	return &ColoredHandler{
		Handler: h.Handler.WithGroup(name),
		w: h.w,
	}
}

func NewLogger(level slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := NewColoredHandler(io.Writer(os.Stdout), opts)
	return slog.New(handler)
}

var Log *slog.Logger = NewLogger(slog.LevelDebug)