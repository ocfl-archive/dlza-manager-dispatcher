package configuration

import (
	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/filesystem/v2/pkg/vfsrw"
	"github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/stashconfig"
	loaderConfig "go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"io/fs"
	"os"
)

type DispatcherConfig struct {
	LocalAddr               string                `toml:"localaddr"`
	CycleLength             int                   `toml:"cyclelength"`
	Domain                  string                `toml:"domain"`
	ExternalAddr            string                `toml:"externaladdr"`
	IIIF                    string                `toml:"iiif"`
	IIIFPrefix              string                `toml:"iiifprefix"`
	IIIFBaseAction          string                `toml:"iiifbaseaction"`
	JWTKey                  string                `toml:"jwtkey"`
	JWTAlg                  []string              `toml:"jwtalg"`
	ResolverAddr            string                `toml:"resolveraddr"`
	ResolverTimeout         config.Duration       `toml:"resolvertimeout"`
	ResolverNotFoundTimeout config.Duration       `toml:"resolvernotfoundtimeout"`
	ClientTLS               *loaderConfig.Config  `toml:"client"`
	LogFile                 string                `toml:"logfile"`
	LogLevel                string                `toml:"loglevel"`
	GRPCClient              map[string]string     `toml:"grpcclient"`
	VFS                     map[string]*vfsrw.VFS `toml:"vfs"`
	Log                     stashconfig.Config    `toml:"log"`
	ActionTemplateTimeout   config.Duration       `toml:"actiontemplatetimeout"`
	CollectionCacheTimeout  config.Duration       `toml:"collectioncachetimeout"`
	CollectionCacheSize     int                   `toml:"collectioncachesize"`
	ItemCacheSize           int                   `toml:"itemcachesize"`
	AmountOfWorkers         int                   `toml:"amountofworkers"`
	TimeToWaitWorker        int                   `toml:"timetowaitworker"`
	WorkerWaitingTime       int                   `toml:"workerwaitingtime"`
}

func LoadDispatcherConfig(fSys fs.FS, fp string, conf *DispatcherConfig) error {
	if _, err := fs.Stat(fSys, fp); err != nil {
		path, err := os.Getwd()
		if err != nil {
			return errors.Wrap(err, "cannot get current working directory")
		}
		fSys = os.DirFS(path)
		fp = "dispatcher.toml"
	}
	data, err := fs.ReadFile(fSys, fp)
	if err != nil {
		return errors.Wrapf(err, "cannot read file [%v] %s", fSys, fp)
	}
	_, err = toml.Decode(string(data), conf)
	if err != nil {
		return errors.Wrapf(err, "error loading config file %v", fp)
	}
	return nil
}
