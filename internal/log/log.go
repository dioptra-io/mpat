package log

import "github.com/sirupsen/logrus"

type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelNormal
	LevelSilent
)

var (
	log      = logrus.New()
	logLevel LogLevel
)

func Init() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
}

func GetLogger() *logrus.Logger {
	return log
}

func SetLogLevel(l LogLevel) {
	switch l {
	case LevelDebug:
		log.SetLevel(logrus.DebugLevel)
	case LevelNormal:
		log.SetLevel(logrus.InfoLevel)
	case LevelSilent:
		log.SetLevel(logrus.PanicLevel)
	}
}

func GetLogLevel() LogLevel {
	return logLevel
}
