package service

import (
	"context"
	zw "github.com/je4/utils/v2/pkg/zLogger"
	"github.com/pkg/errors"
	pb "gitlab.switch.ch/ub-unibas/dlza/dlza-manager/dlzamanagerproto"
	handlerClient "gitlab.switch.ch/ub-unibas/dlza/microservices/dlza-manager-handler/handlerproto"
	storageHandlerClient "gitlab.switch.ch/ub-unibas/dlza/microservices/dlza-manager-storage-handler/storagehandlerproto"
	"strconv"
	"time"
)

type DispatcherHandlerService struct {
	ClientDispatcherHandler        handlerClient.DispatcherHandlerServiceClient
	ClientDispatcherStorageHandler storageHandlerClient.DispatcherStorageHandlerServiceClient
	Logger                         zw.ZWrapper
}

func NewDispatcherHandlerService(clientDispatcherHandler handlerClient.DispatcherHandlerServiceClient, clientDispatcherStorageHandler storageHandlerClient.DispatcherStorageHandlerServiceClient, logger zw.ZWrapper) *DispatcherHandlerService {
	return &DispatcherHandlerService{ClientDispatcherHandler: clientDispatcherHandler, ClientDispatcherStorageHandler: clientDispatcherStorageHandler, Logger: logger}
}

func (d *DispatcherHandlerService) GetLowQualityCollectionsAndAct() error {
	c := context.Background()
	cont, cancel := context.WithTimeout(c, 10000*time.Second)
	defer cancel()

	collectionAliases, err := d.ClientDispatcherHandler.GetLowQualityCollections(cont, &pb.NoParam{})

	if err != nil {
		return errors.Wrapf(err, "cannot get LowQualityCollections")
	}
	length := len(collectionAliases.CollectionAliases)
	if length != 0 {
		d.Logger.Info("Trying to improve quality for "+strconv.Itoa(length)+" collections", time.Now())
		_, err = d.ClientDispatcherStorageHandler.ChangeQualityForCollections(cont, collectionAliases)
		if err != nil {
			return errors.Wrapf(err, "cannot change quality of collections with low quality")
		}
	} else {
		d.Logger.Info("All collections have enough of locations to fit the quality requirements", time.Now())
	}
	return nil
}
