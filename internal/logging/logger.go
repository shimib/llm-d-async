package logging

import (
	"flag"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DEFAULT = 2
	VERBOSE = 3
	DEBUG   = 4
	TRACE   = 5
)

var (
	// Log is the global logger.
	Log logr.Logger
)

// Options contains the configuration for the logger.
type Options struct {
	// Development enables development mode (stacktraces, etc).
	Development bool
	// Level is the log level.
	Level zapcore.Level
}

// BindFlags binds the logging options to the flags.
func (o *Options) BindFlags(fs *flag.FlagSet) {
	fs.BoolVar(&o.Development, "zap-devel", o.Development, "Enable development mode")
}

// InitLogging initializes the logger with zap backend.
func InitLogging(opts *Options, logVerbosity int) {
	// Unless -zap-log-level is explicitly set, use -v
	// Note: In the original code, it checked for "zap-log-level" flag.
	// Since we are not using controller-runtime's zap anymore, we can simplify or keep it.

	lvl := zapcore.Level(-1 * logVerbosity)

	zapConfig := zap.NewProductionConfig()
	if opts.Development {
		zapConfig = zap.NewDevelopmentConfig()
	}
	zapConfig.Level = zap.NewAtomicLevelAt(lvl)

	z, err := zapConfig.Build(zap.AddCaller())
	if err != nil {
		panic(err)
	}

	Log = zapr.NewLogger(z)
}

// Sync flushes any buffered log entries.
func Sync() error {
	// Not easy to sync via logr, but we can store the zap logger if needed.
	return nil
}
