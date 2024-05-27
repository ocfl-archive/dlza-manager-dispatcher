package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/rs/zerolog"
	"gitlab.switch.ch/ub-unibas/dlza/microservices/dlza-manager-dispatcher/configuration"
	"gitlab.switch.ch/ub-unibas/dlza/microservices/dlza-manager-dispatcher/data/certs"
	"gitlab.switch.ch/ub-unibas/dlza/microservices/dlza-manager-dispatcher/service"
	handlerClient "gitlab.switch.ch/ub-unibas/dlza/microservices/dlza-manager-handler/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"io/fs"
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
	/*
	   // Insecure (without TLS)
	   	clientDispatcherHandler, connectionDispatcherHandler, err := handlerClient.NewDispatcherHandlerClient(configObj.Handler.Host+":"+strconv.Itoa(configObj.Handler.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	   	if err != nil {
	   		log.Fatalf("did not connect: %v", err)
	   	}

	*/

	var cert tls.Certificate
	var addCA []*x509.Certificate
	if configObj.GrpcConf.TlsCert == "" {
		certBytes, err := fs.ReadFile(certs.CertFS, "client-cert.pem")
		if err != nil {
			log.Fatalf("cannot read internal cert %s/%v", "client-cert.pem", err)
		}
		keyBytes, err := fs.ReadFile(certs.CertFS, "client-key.pem")
		if err != nil {
			log.Fatalf("cannot read internal key %s/%v", "client-key.pem", err)
		}
		if cert, err = tls.X509KeyPair(certBytes, keyBytes); err != nil {
			log.Fatalf("cannot create internal cert. err: %v", err)
		}

		rootCABytes, err := fs.ReadFile(certs.CertFS, "ca-cert.pem")
		if err != nil {
			log.Fatalf("cannot read root ca %s/%v", "ca-cert.pem", err)
		}
		block, _ := pem.Decode(rootCABytes)
		if block == nil {
			log.Fatalf("cannot decode root ca. err: %v", err)
		}
		rootCA, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			log.Fatalf("cannot parse root ca. err: %v", err)
		}
		addCA = append(addCA, rootCA)
	} else {
		certLoad, err := tls.LoadX509KeyPair(configObj.GrpcConf.TlsCert, configObj.GrpcConf.TlsKey)
		if err != nil {
			log.Fatalf("cannot load key pair %s - %s. err: %v", configObj.GrpcConf.TlsCert, configObj.GrpcConf.TlsKey, err)
		}
		cert = certLoad
		if configObj.GrpcConf.RootCA != nil {
			for _, caName := range configObj.GrpcConf.RootCA {
				rootCABytes, err := os.ReadFile(caName)
				if err != nil {
					log.Fatalf("cannot read root ca %s. err: %v", caName, err)
				}
				block, _ := pem.Decode(rootCABytes)
				if block == nil {
					log.Fatalf("cannot decode root ca. err: %v", err)
				}
				rootCA, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					log.Fatalf("cannot parse root ca. err: %v", err)
				}
				addCA = append(addCA, rootCA)
			}
		}
	}
	ca := x509.NewCertPool()
	for _, certPool := range addCA {
		ca.AddCert(certPool)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      ca,
	}

	clientDispatcherHandler, connectionDispatcherHandler, err := handlerClient.NewDispatcherHandlerClient(configObj.Handler.Host+":"+strconv.Itoa(configObj.Handler.Port), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer connectionDispatcherHandler.Close()

	/*
		//////DispatcherStorageHandler gRPC connection

		clientDispatcherStorageHandler, connectionDispatcherStorageHandler, err := storageHandlerClient.NewDispatcherStorageHandlerClient(configObj.StorageHandler.Host+":"+strconv.Itoa(configObj.StorageHandler.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer connectionDispatcherStorageHandler.Close()

	*/

	dispatcherHandlerService := service.NewDispatcherHandlerService(clientDispatcherHandler, nil)

	for {
		err = dispatcherHandlerService.GetLowQualityCollectionsAndAct()
		if err != nil {
			daLogger.Errorf("error in GetLowQualityCollectionsAndAct methos: %v", err)
		}
		time.Sleep(time.Duration(configObj.CycleLength) * time.Second)
		fmt.Print("Check\n")
	}
}
