package go_library

import (
	"fmt"
	"os"

	"github.com/orandin/lumberjackrus"
	"github.com/sirupsen/logrus"
)

func LoggerInit() {

	logrus.SetFormatter(&logrus.TextFormatter{})
	logrus.SetLevel(logrus.DebugLevel)

	setting_log_max_size := int(ToInt64(os.Getenv("LOG_MAX_SIZE"), 0))
	if setting_log_max_size == 0 {
		setting_log_max_size = 50
	}

	setting_log_max_backup := int(ToInt64(os.Getenv("LOG_MAX_BACKUP"), 0))
	if setting_log_max_backup == 0 {
		setting_log_max_backup = 30
	}

	setting_log_max_age := int(ToInt64(os.Getenv("LOG_MAX_AGE"), 0))
	if setting_log_max_age == 0 {
		setting_log_max_age = 30
	}

	hook, err := lumberjackrus.NewHook(
		&lumberjackrus.LogFile{
			Filename:   os.Getenv("LOG_FOLDER_PATH") + os.Getenv("APP_ID") + "_general.log",
			MaxSize:    setting_log_max_size,
			MaxBackups: setting_log_max_backup,
			MaxAge:     setting_log_max_age,
			Compress:   true,
			LocalTime:  true,
		},
		logrus.InfoLevel,
		&logrus.TextFormatter{},
		&lumberjackrus.LogFileOpts{
			logrus.InfoLevel: &lumberjackrus.LogFile{
				Filename:   os.Getenv("LOG_FOLDER_PATH") + os.Getenv("APP_ID") + "_info.log",
				MaxSize:    setting_log_max_size,   // optional
				MaxBackups: setting_log_max_backup, // optional
				MaxAge:     setting_log_max_age,    // optional
				Compress:   true,                   // optional
				LocalTime:  true,                   // optional
			},
			logrus.ErrorLevel: &lumberjackrus.LogFile{
				Filename:   os.Getenv("LOG_FOLDER_PATH") + os.Getenv("APP_ID") + "_error.log",
				MaxSize:    setting_log_max_size,   // optional
				MaxBackups: setting_log_max_backup, // optional
				MaxAge:     setting_log_max_age,    // optional
				Compress:   true,                   // optional
				LocalTime:  true,                   // optional
			},
		},
	)

	if err != nil {
		fmt.Println("Error Logger : ", err.Error())
	}

	logrus.AddHook(hook)
}

func AddLogInfo(message string) {
	logrus.Info(message)
}

func AddLogInfoWithFields(message string, param map[string]interface{}) {
	logrus.WithFields(param).Info(message)
}

func AddLogWarn(message string) {
	logrus.Warn(message)
}

func AddLogWarnWithFields(message string, param map[string]interface{}) {
	logrus.WithFields(param).Warn(message)
}

func AddLogError(message string) {
	logrus.Error(message)
}

func AddLogErrorWithFields(message string, param map[string]interface{}) {
	logrus.WithFields(param).Error(message)
}
