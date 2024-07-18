package main

import (
	"flag"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager-dispatcher/configuration"
	"github.com/ocfl-archive/dlza-manager-dispatcher/service"
	handlerClient "github.com/ocfl-archive/dlza-manager-handler/client"
	storageHandlerClient "github.com/ocfl-archive/dlza-manager-storage-handler/client"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

var configParam = flag.String("config", "", "config file in toml format")

func main() {
	flag.Parse()

	configObj := configuration.GetConfig(*configParam)

	// create logger instance
	var out io.Writer = os.Stdout
	if string(configObj.Logging.LogFile) != "" {
		fp, err := os.OpenFile(string(configObj.Logging.LogFile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open logfile %s: %v", string(configObj.Logging.LogFile), err)
		}
		defer fp.Close()
		out = fp
	}

	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(output).With().Timestamp().Logger()
	_logger.Level(zLogger.LogLevel(string(configObj.Logging.LogLevel)))
	var logger zLogger.ZLogger = &_logger
	daLogger := zLogger.NewZWrapper(logger)

	//////DispatcherHandler gRPC connection

	clientDispatcherHandler, connectionDispatcherHandler, err := handlerClient.NewDispatcherHandlerClient(configObj.Handler.Host+":"+strconv.Itoa(configObj.Handler.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer connectionDispatcherHandler.Close()

	//////DispatcherStorageHandler gRPC connection

	clientDispatcherStorageHandler, connectionDispatcherStorageHandler, err := storageHandlerClient.NewDispatcherStorageHandlerClient(configObj.StorageHandler.Host+":"+strconv.Itoa(configObj.StorageHandler.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer connectionDispatcherStorageHandler.Close()

	dispatcherHandlerService := service.NewDispatcherHandlerService(clientDispatcherHandler, clientDispatcherStorageHandler, daLogger)

	for {
		err = dispatcherHandlerService.GetLowQualityCollectionsAndAct()
		if err != nil {
			daLogger.Errorf("error in GetLowQualityCollectionsAndAct methos: %v", err)
		}
		time.Sleep(time.Duration(configObj.CycleLength) * time.Second)
	}
}
