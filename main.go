package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"emperror.dev/errors"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager-dispatcher/configuration"
	handlerClientProto "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	storageHandlerClientProto "github.com/ocfl-archive/dlza-manager-storage-handler/storagehandlerproto"
	"github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/ocfl-archive/dlza-manager/models"
	dlzaService "github.com/ocfl-archive/dlza-manager/service"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"go.ub.unibas.ch/cloud/miniresolver/v2/pkg/resolver"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	errorStatus  = "error"
	okStatus     = "ok"
	deleteStatus = "to delete"
	notAvailable = "not available"
	deprecated   = "deprecated"
	newStatus    = "new"
)

type Cash struct {
	mu         sync.Mutex
	ObjectCash map[string]*dlzamanagerproto.Object
}

func (c *Cash) Add(obj *dlzamanagerproto.Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ObjectCash[obj.Id] = obj
}

func (c *Cash) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ObjectCash)
}

func (c *Cash) GetKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Keys(c.ObjectCash)
}

func (c *Cash) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.ObjectCash, id)
}

var cash *Cash

type Job struct {
	ObjectToWorkWith         *dlzamanagerproto.Object
	RelevantStorageLocations *dlzamanagerproto.StorageLocations
}

var workerWaitingTime int

func worker(id int, in <-chan Job, dispatcherHandlerServiceClient handlerClientProto.DispatcherHandlerServiceClient,
	dispatcherStorageHandlerServiceClient storageHandlerClientProto.DispatcherStorageHandlerServiceClient, wg *sync.WaitGroup, logger zLogger.ZLogger) {
	defer wg.Done()
	for {
		select {
		case obj, ok := <-in:
			if !ok {
				logger.Info().Msgf("Data channel is closed. Worker ID: %d", id)
				return
			}
			err := checkObjectInstancesDistributionAndReact(context.Background(), dispatcherHandlerServiceClient, dispatcherStorageHandlerServiceClient, obj, logger)
			if err != nil {
				logger.Error().Msgf("cannot checkObjectInstancesDistributionAndReact for object with ID %s, err: %v", obj.ObjectToWorkWith.Id, err)
				cash.Delete(obj.ObjectToWorkWith.Id)
				continue
			}
			logger.Info().Msgf("Worker ID: %d finished to process object with ID: %s", id, obj.ObjectToWorkWith.Id)
			cash.Delete(obj.ObjectToWorkWith.Id)
			logger.Debug().Msgf("Worker ID: %d cleared cash. Cash length: %d", id, cash.Len())
		case <-time.After(time.Duration(workerWaitingTime) * time.Second):
			//logger.Debug().Msgf("Timeout: no value received in %d second. Worker ID: %d", workerWaitingTime, id)
		}
	}
}

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
	resolverClient, err := resolver.NewMiniresolverClientNet(conf.ResolverAddr, conf.NetName, conf.GRPCClient, clientCert, nil, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)
	if err != nil {
		logger.Fatal().Msgf("cannot create resolver client: %v", err)
	}
	defer resolverClient.Close()

	//////DispatcherHandler gRPC connection

	clientDispatcherHandler, err := resolver.NewClient[handlerClientProto.DispatcherHandlerServiceClient](
		resolverClient,
		handlerClientProto.NewDispatcherHandlerServiceClient,
		handlerClientProto.DispatcherHandlerService_ServiceDesc.ServiceName, conf.Domain)
	if err != nil {
		logger.Panic().Msgf("cannot create clientDispatcherHandler grpc client: %v", err)
	}

	//////DispatcherStorageHandler gRPC connection

	clientDispatcherStorageHandler, err := resolver.NewClient[storageHandlerClientProto.DispatcherStorageHandlerServiceClient](
		resolverClient,
		storageHandlerClientProto.NewDispatcherStorageHandlerServiceClient,
		storageHandlerClientProto.DispatcherStorageHandlerService_ServiceDesc.ServiceName, conf.Domain)
	if err != nil {
		logger.Panic().Msgf("cannot create clientDispatcherStorageHandler grpc client: %v", err)
	}
	msg, err := clientDispatcherStorageHandler.ConnectionCheck(context.Background(), &emptypb.Empty{})
	if err != nil {
		logger.Panic().Msgf("Storage Handler connection check was unsuccsessful: %v", err)
	}
	logger.Info().Msg(msg.Id)
	var wg sync.WaitGroup
	cash = &Cash{ObjectCash: make(map[string]*dlzamanagerproto.Object)}
	jobChan := make(chan Job)
	workerWaitingTime = conf.WorkerWaitingTime
	for i := 0; i < conf.AmountOfWorkers; i++ {
		wg.Add(1)
		go worker(i, jobChan, clientDispatcherHandler, clientDispatcherStorageHandler, &wg, logger)
	}

	var end = make(chan struct{}, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {

			tenants, err := clientDispatcherHandler.FindAllTenants(context.Background(), &dlzamanagerproto.NoParam{})
			if err != nil {
				logger.Error().Msgf("cannot FindAllTenants %v", err)
			}
			for _, tenant := range tenants.Tenants {

				collections, err := clientDispatcherHandler.GetCollectionsByTenantId(context.Background(), &dlzamanagerproto.Id{Id: tenant.Id})
				if err != nil {
					logger.Error().Msgf("cannot GetCollectionsByTenantId %v", err)
					continue
				}
				storageLocationsPb, err := clientDispatcherHandler.GetStorageLocationsByTenantId(context.Background(), &dlzamanagerproto.Id{Id: tenant.Id})
				if err != nil {
					logger.Error().Msgf("cannot GetStorageLocationsByTenantId %v", err)
					continue
				}
				for _, collection := range collections.Collections {

					relevantStorageLocationsGrouped := dlzaService.GetCheapestStorageLocationsForQuality(storageLocationsPb, int(collection.Quality))
					if len(relevantStorageLocationsGrouped) == 0 {
						logger.Error().Msgf("collection %s does not have enough storage locations to gain the quality needed", collection.Alias)
						continue
					}
					storageLocationDistribution, err := clientDispatcherHandler.GetExistingStorageLocationsCombinationsForCollectionId(context.Background(), &dlzamanagerproto.Id{Id: collection.Id})
					if err != nil {
						logger.Error().Msgf("cannot GetExistingStorageLocationsCombinationsForCollectionId %v", err)
						continue
					}
					checkDoesNotNeeded := false
					if len(storageLocationDistribution.StorageLocationsCombinationsForCollections) == 1 {
						for index, storageLocation := range relevantStorageLocationsGrouped {
							if !slices.Contains(storageLocationDistribution.StorageLocationsCombinationsForCollections[0].LocationsIds, storageLocation.Group) {
								break
							} else {
								if len(relevantStorageLocationsGrouped)-1 == index {
									checkDoesNotNeeded = true
								}
							}
						}
					}
					if checkDoesNotNeeded {
						continue
					}
					logger.Debug().Msgf("collection with alias %s should be checked for quality", collection.Alias)
					var relevantStorageLocationsGroups []string
					for _, relevantStorageLocation := range relevantStorageLocationsGrouped {
						relevantStorageLocationsGroups = append(relevantStorageLocationsGroups, relevantStorageLocation.Group)
					}
					for {
						object, err := clientDispatcherHandler.GetObjectExceptListOlderThan(context.Background(),
							&dlzamanagerproto.IdsWithSQLInterval{CollectionId: collection.Id, Ids: cash.GetKeys(), CollectionsIds: relevantStorageLocationsGroups})
						if err != nil {
							logger.Debug().Msgf("cannot GetObjectsByCollectionAlias for collection: %s, %v", collection.Alias, err)
							break
						}
						if object.Id == "" {
							logger.Debug().Msgf("All objects from collection with alias %s are archived with quality needed", collection.Alias)
							break
						}
						cash.Add(object)
						jobChan <- Job{ObjectToWorkWith: object, RelevantStorageLocations: &dlzamanagerproto.StorageLocations{StorageLocations: relevantStorageLocationsGrouped}}
						if cash.Len() == conf.AmountOfWorkers {
							for {
								time.Sleep(time.Duration(conf.TimeToWaitWorker) * time.Second)
								if cash.Len() < conf.AmountOfWorkers {
									break
								}
							}
						}
					}
				}
			}
			select {
			case <-end:
				return
			case <-time.After(time.Duration(conf.CycleLength) * time.Second):
				logger.Debug().Msgf("Cycle with length %d seconds finished. Ammount of workers: %d", conf.CycleLength, conf.AmountOfWorkers)
			}

		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)
	close(jobChan)
	close(end)
	wg.Wait()
}

func checkObjectInstancesDistributionAndReact(ctx context.Context, dispatcherHandlerServiceClient handlerClientProto.DispatcherHandlerServiceClient,
	dispatcherStorageHandlerServiceClient storageHandlerClientProto.DispatcherStorageHandlerServiceClient, obj Job, logger zLogger.ZLogger) error {
	objectInstancesChecked := make([]*dlzamanagerproto.ObjectInstance, 0)
	storageLocationsAndObjectInstancesCurrent := make(map[*dlzamanagerproto.ObjectInstance]*dlzamanagerproto.StorageLocation)
	objectInstances, err := dispatcherHandlerServiceClient.GetObjectInstancesByObjectIdPositive(context.Background(), &dlzamanagerproto.Id{Id: obj.ObjectToWorkWith.Id})
	if err != nil {
		logger.Error().Msgf("cannot GetObjectInstancesByObjectIdPositive for object with ID %s, err: %v", obj.ObjectToWorkWith.Id, err)
		return errors.Wrapf(err, "cannot GetObjectInstancesByObjectIdPositive for object with ID %s", obj.ObjectToWorkWith.Id)
	}
	var objectInstanceToCopyFrom *dlzamanagerproto.ObjectInstance
	for index, objectInstanceIter := range objectInstances.ObjectInstances {
		storageLocation, err := dispatcherHandlerServiceClient.GetStorageLocationByObjectInstanceId(ctx, &dlzamanagerproto.Id{Id: objectInstanceIter.Id})
		if err != nil {
			logger.Error().Msgf("cannot GetStorageLocationByObjectInstanceId for object instance with path %s, err: %v", objectInstanceIter.Path, err)
			return errors.Wrapf(err, "cannot GetStorageLocationByObjectInstanceId for object instance with path %s", objectInstanceIter.Path)
		}
		storageLocationsAndObjectInstancesCurrent[objectInstanceIter] = storageLocation
		objectInstancesChecked = append(objectInstancesChecked, objectInstanceIter)
		if storageLocation.FillFirst {
			objectInstanceToCopyFrom = objectInstanceIter
		} else if index == len(objectInstances.ObjectInstances)-1 && objectInstanceToCopyFrom == nil && len(objectInstancesChecked) != 0 {
			objectInstanceToCopyFrom = objectInstancesChecked[0]
		}
	}
	if len(objectInstancesChecked) == 0 {
		logger.Error().Msgf("There is no any object instance to copy from for object with ID %v", objectInstances.ObjectInstances[0].ObjectId)
		return errors.New(fmt.Sprintf("There is no any object instance to copy from for object with ID %v", objectInstances.ObjectInstances[0].ObjectId))
	}
	if objectInstanceToCopyFrom == nil {
		objectInstanceToCopyFrom = objectInstancesChecked[0]
	}
	storageLocationsToCopyTo := dlzaService.GetStorageLocationsToCopyTo(obj.RelevantStorageLocations, maps.Values(storageLocationsAndObjectInstancesCurrent))
	storageLocationsToDeleteFromWithObjectInstances := dlzaService.GetStorageLocationsToDeleteFrom(obj.RelevantStorageLocations, storageLocationsAndObjectInstancesCurrent)

	for _, storageLocationToCopyTo := range storageLocationsToCopyTo {
		storagePartition, err := dispatcherHandlerServiceClient.GetStoragePartitionForLocation(ctx, &dlzamanagerproto.SizeObjectLocation{Size: objectInstanceToCopyFrom.Size, Location: storageLocationToCopyTo, Object: obj.ObjectToWorkWith})
		if err != nil {
			logger.Error().Msgf("cannot get storagePartition for storage location %s, err: %v", storageLocationToCopyTo.Alias, err)
			return errors.Wrapf(err, "cannot get storagePartition for storage location %s", storageLocationToCopyTo.Alias)
		}
		storageLocation, err := dispatcherHandlerServiceClient.GetStorageLocationById(ctx, &dlzamanagerproto.Id{Id: storagePartition.StorageLocationId})
		if err != nil {
			logger.Error().Msgf("cannot GetStorageLocationById for ID %s, err: %v", storagePartition.StorageLocationId, err)
			return errors.Wrapf(err, "cannot GetStorageLocationById for ID %s", storagePartition.StorageLocationId)
		}

		connection := models.Connection{}
		err = json.Unmarshal([]byte(storageLocation.Connection), &connection)
		if err != nil {
			logger.Error().Msgf("error mapping json")
			return errors.Wrapf(err, "error mapping json for storageLocation: %s", storageLocation.Alias)
		}

		pathString := path.Join(connection.Folder, storagePartition.Alias, filepath.Base(objectInstanceToCopyFrom.Path))
		objectInstance := &dlzamanagerproto.ObjectInstance{Path: pathString, Status: "ok", ObjectId: objectInstanceToCopyFrom.ObjectId, StoragePartitionId: storagePartition.Id, Size: objectInstanceToCopyFrom.Size}
		_, err = dispatcherHandlerServiceClient.CreateObjectInstance(ctx, objectInstance)
		if err != nil {
			logger.Error().Msgf("Could not create objectInstance for object with ID: %s. err: %v", objectInstanceToCopyFrom.ObjectId, err)
			return errors.Wrapf(err, "Could not create objectInstance for object with ID: %s", objectInstanceToCopyFrom.ObjectId)
		}

		_, err = dispatcherStorageHandlerServiceClient.CopyArchiveTo(ctx, &dlzamanagerproto.CopyFromTo{CopyTo: pathString, CopyFrom: objectInstanceToCopyFrom.Path})
		if err != nil {
			logger.Error().Msgf("cannot CopyArchiveTo for object instance with path %s to storage location %s, err: %v", objectInstanceToCopyFrom.Path, storageLocation.Alias, err)
			return errors.Wrapf(err, "cannot CopyArchiveTo for object instance with path %s to storage location %s", objectInstanceToCopyFrom.Path, storageLocation.Alias)
		}
	}
	for objectInstanceToDelete := range storageLocationsToDeleteFromWithObjectInstances {
		objectInstanceToDelete.Status = deleteStatus
		_, err := dispatcherHandlerServiceClient.UpdateObjectInstance(ctx, objectInstanceToDelete)
		if err != nil {
			logger.Error().Msgf("cannot UpdateObjectInstance with ID %s, err: %v ", objectInstanceToDelete.Id, err)
			return errors.Wrapf(err, "cannot UpdateObjectInstance with ID %s", objectInstanceToDelete.Id)
		}
	}
	if len(storageLocationsToCopyTo) != 0 {
		objectInstancesNew, err := dispatcherHandlerServiceClient.GetObjectsInstancesByObjectId(ctx, &dlzamanagerproto.Id{Id: objectInstances.ObjectInstances[0].ObjectId})
		if err != nil {
			logger.Error().Msgf("cannot GetObjectsInstancesByObjectId for object with ID %s, err %v", objectInstances.ObjectInstances[0].ObjectId, err)
			return errors.Wrapf(err, "cannot GetObjectsInstancesByObjectId for object with ID %s", objectInstances.ObjectInstances[0].ObjectId)
		}
		objectInstances = objectInstancesNew
	}
	for _, objectInstance := range objectInstances.ObjectInstances {
		if objectInstance.Status == notAvailable || objectInstance.Status == errorStatus || objectInstance.Status == deprecated {
			objectInstance.Status = deleteStatus
			_, err := dispatcherHandlerServiceClient.UpdateObjectInstance(ctx, objectInstance)
			if err != nil {
				logger.Error().Msgf("cannot UpdateObjectInstance for object with ID %s, err %v", objectInstance.ObjectId, err)
				return errors.Wrapf(err, "cannot UpdateObjectInstance for object with ID %s", objectInstance.ObjectId)
			}
		}
	}
	return nil
}
