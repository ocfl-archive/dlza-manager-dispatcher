package service

import (
	"context"
	zw "github.com/je4/utils/v2/pkg/zLogger"
	handlerClient "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	storageHandlerClient "github.com/ocfl-archive/dlza-manager-storage-handler/storagehandlerproto"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/pkg/errors"
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
	ctx := context.Background()

	collectionsWithObjectIdsToImproveQuality, err := d.ClientDispatcherHandler.GetLowQualityCollectionsWithObjectIds(ctx, &pb.NoParam{})

	if err != nil {
		return errors.Wrapf(err, "cannot get GetLowQualityCollectionsWithObjectIds")
	}
	length := len(collectionsWithObjectIdsToImproveQuality.CollectionAliases)
	if length != 0 {
		d.Logger.Info("Trying to improve quality for "+strconv.Itoa(length)+" collections", time.Now())
		_, err = d.ClientDispatcherStorageHandler.ChangeQualityForCollectionWithObjectIds(ctx, collectionsWithObjectIdsToImproveQuality)
		if err != nil {
			return errors.Wrapf(err, "cannot change quality of collections with low quality")
		}
	} else {
		d.Logger.Info("All collections have enough of locations to fit the quality requirements", time.Now())
	}
	return nil
}
