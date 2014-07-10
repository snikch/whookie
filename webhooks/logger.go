// Logger is a simple logging interface.
package main

import (
	"io"
	"log"
	"log/syslog"
	"os"
)

var logger Logger

func init() {
	logger = Logger{}
	if os.Getenv("LOG_OUTPUT") == "syslog" {
		logger.info, _ = syslog.NewLogger(syslog.LOG_INFO, log.LstdFlags)
		logger.notice, _ = syslog.NewLogger(syslog.LOG_NOTICE, log.LstdFlags)
		logger.warning, _ = syslog.NewLogger(syslog.LOG_WARNING, log.LstdFlags)
		logger.error, _ = syslog.NewLogger(syslog.LOG_ERR, log.LstdFlags)
	} else {
		logger.info = log.New(os.Stdout, "INFO ", log.LstdFlags)
		logger.notice = log.New(os.Stdout, "NOTICE ", log.LstdFlags)
		logger.warning = log.New(os.Stdout, "WARNING ", log.LstdFlags)
		logger.error = log.New(os.Stderr, "ERROR ", log.LstdFlags)
	}
}

type Logger struct {
	info    *log.Logger
	notice  *log.Logger
	warning *log.Logger
	error   *log.Logger
}

func SetLoggerOutput(l Logger, w io.Writer) {
	l.info = log.New(w, "INFO ", log.LstdFlags)
	l.notice = log.New(w, "NOTICE ", log.LstdFlags)
	l.warning = log.New(w, "WARNING ", log.LstdFlags)
	l.error = log.New(w, "ERROR ", log.LstdFlags)
}

func (l Logger) Info(messages ...interface{}) {
	l.info.Print(messages...)
}

func (l Logger) Notice(messages ...interface{}) {
	l.notice.Print(messages...)
}

func (l Logger) Warning(messages ...interface{}) {
	l.warning.Print(messages...)
}

func (l Logger) Error(messages ...interface{}) {
	l.error.Print(messages...)
}

func (l Logger) Infof(str string, messages ...interface{}) {
	l.info.Printf(str, messages...)
}

func (l Logger) Noticef(str string, messages ...interface{}) {
	l.notice.Printf(str, messages...)
}

func (l Logger) Warningf(str string, messages ...interface{}) {
	l.warning.Printf(str, messages...)
}

func (l Logger) Errorf(str string, messages ...interface{}) {
	l.error.Printf(str, messages...)
}
