package configuration

import (
	"github.com/jinzhu/configor"
	"log"
)

type Service struct {
	ServiceName string `yaml:"service_name" toml:"ServiceName"`
	Host        string `yaml:"host" toml:"Host"`
	Port        int    `yaml:"port" toml:"Port"`
}

type Logging struct {
	LogLevel string
	LogFile  string
}
type GrpcAuth struct {
	TlsCert string   `toml:"certificate"`    // TLS Certificate
	TlsKey  string   `toml:"certificatekey"` // TLS Certificate Private Key
	RootCA  []string `toml:"rootca"`         // additional root CA to trust

}

type Config struct {
	Handler        Service  `yaml:"handler" toml:"Handler"`
	StorageHandler Service  `yaml:"storage-handler" toml:"StorageHandler"`
	Logging        Logging  `yaml:"logging" toml:"Logging"`
	Dispatcher     Service  `yaml:"dispatcher" toml:"Dispatcher"`
	CycleLength    int      `yaml:"cycle-length" toml:"CycleLength"`
	GrpcConf       GrpcAuth `yaml:"grpc-conf" toml:"GrpcConf"`
}

// GetConfig creates a new config from a given environment
func GetConfig(configFile string) Config {
	conf := Config{}
	if configFile == "" {
		configFile = "config.yml"
	}
	err := configor.Load(&conf, configFile)
	if err != nil {
		log.Fatal(err)
	}
	return conf
}
