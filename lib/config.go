package lib

import (
	"fmt"
	"github.com/go-ini/ini"
)

type AppConfig struct {
	KafkaBrokers    []string
	ConsumerGroupID string
	AlarmTopics     []string
	MongodbURL      string
	WorkerNum       int
	BulkDataBuffer  int
	MySqlURL        string
}

var AppCfg = AppConfig{}

func InitCfg() {

	iniFile, err := ini.Load("config.ini")
	if err != nil {
		panic("读取配置失败")
	}

	mapTo(iniFile, "app", &AppCfg)

}

func mapTo(iniFile *ini.File, section string, v interface{}) {

	err := iniFile.Section(section).MapTo(v)
	if err != nil {
		panic(fmt.Sprintf("mapTo %s: Setting Error", section))
	}

	fmt.Printf("%s: %+v \n", section, v)
}
