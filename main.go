package main

import (
	"crypto/tls"
	"flag"
	"github.com/je4/certloader/v2/pkg/loader"
	"github.com/je4/miniresolver/v2/pkg/resolver"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager-dispatcher/configuration"
	"github.com/ocfl-archive/dlza-manager-dispatcher/service"
	handlerClient "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	storageHandlerClient "github.com/ocfl-archive/dlza-manager-storage-handler/client"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"
)

var configParam = flag.String("config", "", "config file in toml format")

func main() {
	flag.Parse()

	var cfgFS fs.FS
	var cfgFile string
	if *configParam != "" {
		cfgFS = os.DirFS(filepath.Dir(*configParam))
		cfgFile = filepath.Base(*configParam)
	} else {
		cfgFS = configuration.ConfigFS
		cfgFile = "dispatcher.toml"
	}

	conf := &configuration.DispatcherConfig{
		LocalAddr: "localhost:8443",
		//ResolverTimeout: config.Duration(10 * time.Minute),
		ExternalAddr:            "https://localhost:8443",
		LogLevel:                "DEBUG",
		ResolverTimeout:         configutil.Duration(10 * time.Minute),
		ResolverNotFoundTimeout: configutil.Duration(10 * time.Second),
		ActionTemplateTimeout:   configutil.Duration(120 * time.Second),
		CollectionCacheTimeout:  configutil.Duration(10 * time.Minute),
		CollectionCacheSize:     30,
		ItemCacheSize:           1000,
		ClientTLS: &loader.Config{
			Type: "DEV",
		},
	}

	if err := configuration.LoadDispatcherConfig(cfgFS, cfgFile, conf); err != nil {
		log.Fatalf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}

	// create logger instance
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatalf("cannot create client loader: %v", err)
		}
		defer loggerLoader.Close()
	}

	_logger, _logstash, _logfile, err := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if err != nil {
		log.Fatalf("cannot create logger: %v", err)
	}
	if _logstash != nil {
		defer _logstash.Close()
	}
	if _logfile != nil {
		defer _logfile.Close()
	}
	l2 := _logger.With().Timestamp().Str("host", hostname).Str("addr", conf.LocalAddr).Logger() //.Output(output)
	var logger zLogger.ZLogger = &l2

	clientCert, clientLoader, err := loader.CreateClientLoader(conf.ClientTLS, logger)
	if err != nil {
		logger.Panic().Msgf("cannot create client loader: %v", err)
	}
	defer clientLoader.Close()

	logger.Info().Msgf("resolver address is %s", conf.ResolverAddr)
	resolverClient, err := resolver.NewMiniresolverClient(conf.ResolverAddr, conf.GRPCClient, clientCert, nil, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)
	if err != nil {
		logger.Fatal().Msgf("cannot create resolver client: %v", err)
	}
	defer resolverClient.Close()

	//////DispatcherHandler gRPC connection

	clientDispatcherHandler, err := resolver.NewClient[handlerClient.DispatcherHandlerServiceClient](resolverClient, handlerClient.NewDispatcherHandlerClient, mediaserverproto.Database_ServiceDesc.ServiceName, conf.Domain)
	if err != nil {
		logger.Panic().Msgf("cannot create dispatcher grpc client: %v", err)
	}
	resolver.DoPing(clientDispatcherHandler, logger)
	/*
		clientDispatcherHandler, connectionDispatcherHandler, err := handlerClient.NewDispatcherHandlerClient(conf.HandlerHost+conf.HandlerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer connectionDispatcherHandler.Close()

	*/

	//////DispatcherStorageHandler gRPC connection

	clientDispatcherStorageHandler, connectionDispatcherStorageHandler, err := storageHandlerClient.NewDispatcherStorageHandlerClient(conf.StorageHandlerHost+conf.StorageHandlerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer connectionDispatcherStorageHandler.Close()

	dispatcherHandlerService := service.NewDispatcherHandlerService(clientDispatcherHandler, clientDispatcherStorageHandler, logger)

	for {
		err = dispatcherHandlerService.GetLowQualityCollectionsAndAct()
		if err != nil {
			logger.Error().Msgf("error in GetLowQualityCollectionsAndAct methos: %v", err)
		}
		time.Sleep(time.Duration(conf.CycleLength) * time.Second)
	}
}
