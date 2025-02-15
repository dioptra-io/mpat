package log

import "github.com/sirupsen/logrus"

var log = logrus.New()

func Init() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	log.SetLevel(logrus.InfoLevel)
}

func GetLogger() *logrus.Logger {
	return log
}

func SetSilent() {
	log.SetLevel(logrus.PanicLevel)
}
