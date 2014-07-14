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
	flags := log.LstdFlags
	if os.Getenv("LOG_OUTPUT") == "syslog" {
		logger.info, _ = syslog.NewLogger(syslog.LOG_INFO, flags)
		logger.notice, _ = syslog.NewLogger(syslog.LOG_NOTICE, flags)
		logger.warning, _ = syslog.NewLogger(syslog.LOG_WARNING, flags)
		logger.error, _ = syslog.NewLogger(syslog.LOG_ERR, flags)
	} else {
		logger.info = log.New(os.Stdout, "INFO ", flags)
		logger.notice = log.New(os.Stdout, "NOTICE ", flags)
		logger.warning = log.New(os.Stdout, "WARNING ", flags)
		logger.error = log.New(os.Stderr, "ERROR ", flags)
	}
}

type Logger struct {
	info    *log.Logger
	notice  *log.Logger
	warning *log.Logger
	error   *log.Logger
}

func SetLoggerOutput(l Logger, w io.Writer) {
	flags := log.LstdFlags
	l.info = log.New(w, "INFO ", flags)
	l.notice = log.New(w, "NOTICE ", flags)
	l.warning = log.New(w, "WARNING ", flags)
	l.error = log.New(w, "ERROR ", flags)
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
