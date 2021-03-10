package sqlx

import (
	"bytes"
	"fmt"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
)

var logger Logger = loggerX{}

type Logger interface {
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

// Set Logger
func SetLogger(l Logger) {
	logger = l
}

type loggerX struct{}

func (l loggerX) Infof(format string, v ...interface{}) {
	logx.Infof(format, v...)
}

func (l loggerX) Warnf(format string, v ...interface{}) {
	logx.Slowf(format, v...)
}

func (l loggerX) Errorf(format string, v ...interface{}) {
	logx.Errorf(format, v...)
}

//func (l loggerX) WithDuration(d time.Duration) logx.Logger {
//	return logx.WithDuration(d)
//}

func calcDuration(duration time.Duration) string {
	return fmt.Sprintf(`, duration: %.1fms`, float32(duration)/float32(time.Millisecond))
}

func logWithDurationInfo(duration time.Duration, format string, v ...interface{}) {
	if isLogx, ok := logger.(interface {
		WithDuration(d time.Duration) logx.Logger
	}); ok {
		isLogx.WithDuration(duration).Infof(format, v...)
	} else {
		buffer := bytes.NewBufferString(format)
		buffer.WriteString(calcDuration(duration))
		logger.Infof(buffer.String(), v...)
	}
}

func logWithDurationSlow(duration time.Duration, format string, v ...interface{}) {
	if isLogx, ok := logger.(interface {
		WithDuration(d time.Duration) logx.Logger
	}); ok {
		isLogx.WithDuration(duration).Slowf(format, v...)
	} else {
		buffer := bytes.NewBufferString(format)
		buffer.WriteString(calcDuration(duration))
		buffer.WriteByte(',')
		logger.Warnf(buffer.String(), v...)
	}
}

func logInstanceError(cfg Conf, err error) {
	db := fmt.Sprintf("%s#%s:%d", cfg.GetDriver(), cfg.GetHost(), cfg.GetProt())
	logger.Errorf("Error on getting sql instance of %s: %v", db, err)
}

func logSqlError(stmt string, err error) {
	if err != nil && err != ErrNotFound {
		logger.Errorf("stmt: %s, error: %s", stmt, err.Error())
	}
}
