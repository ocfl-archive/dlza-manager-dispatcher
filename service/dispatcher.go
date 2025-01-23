package service

import (
	"context"
	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	handlerClient "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	storageHandlerClient "github.com/ocfl-archive/dlza-manager-storage-handler/storagehandlerproto"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"strconv"
	"time"
)

type DispatcherHandlerService struct {
	ClientDispatcherHandler        handlerClient.DispatcherHandlerServiceClient
	ClientDispatcherStorageHandler storageHandlerClient.DispatcherStorageHandlerServiceClient
	Logger                         zLogger.ZLogger
}

func NewDispatcherHandlerService(clientDispatcherHandler handlerClient.DispatcherHandlerServiceClient, clientDispatcherStorageHandler storageHandlerClient.DispatcherStorageHandlerServiceClient, logger zLogger.ZLogger) *DispatcherHandlerService {
	return &DispatcherHandlerService{ClientDispatcherHandler: clientDispatcherHandler, ClientDispatcherStorageHandler: clientDispatcherStorageHandler, Logger: logger}
}

func (d *DispatcherHandlerService) GetLowQualityCollectionsAndAct() error {
	c := context.Background()
	cont, cancel := context.WithTimeout(c, 10000*time.Second)
	defer cancel()

	collectionAliases, err := d.ClientDispatcherHandler.GetLowQualityCollectionsWithObjectIds(cont, &pb.NoParam{})

	if err != nil {
		return errors.Wrapf(err, "cannot GetLowQualityCollectionsWithObjectIds")
	}
	length := len(collectionAliases.CollectionAliases)
	if length != 0 {
		d.Logger.Info().Msgf("Trying to improve quality for "+strconv.Itoa(length)+" collections", time.Now())
		_, err = d.ClientDispatcherStorageHandler.ChangeQualityForCollectionWithObjectIds(cont, collectionAliases)
		if err != nil {
			return errors.Wrapf(err, "cannot change quality of collections with low quality")
		}
	} else {
		d.Logger.Info().Msgf("All collections have enough of locations to fit the quality requirements", time.Now())
	}
	return nil
}
