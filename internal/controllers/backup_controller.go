package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/bastion/internal/config"
	"github.com/bastion/internal/dispatcher"
	"github.com/bastion/internal/gc"
	"github.com/bastion/internal/hash"
	"github.com/bastion/internal/storage"
	"github.com/bastion/internal/storage/filesystem"
	"github.com/bastion/internal/worker"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsinformer "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// BackupController sets up dynamic informers for CRDs and handles backup + GC of custom resources.
type BackupController struct {
	Dispatcher         *dispatcher.Dispatcher                       // Central dispatcher that manages informer and worker wiring
	Hasher             hash.Hasher                                  // Responsible for hashing resource manifests
	StoreFactory       func(base string) storage.Storage            // Factory to provide a storage writer
	InformerFactory    dynamicinformer.DynamicSharedInformerFactory // Dynamic informer factory for CR instances
	MaxRetries         int                                          // Max number of retries for failed backup attempts
	BaseDir            string                                       // Base directory for storing backups
	totalRegisteredGVK int                                          // Count of active GVK informers (for monitoring/logging)
	GcRetain           time.Duration
}

// NewBackupController constructs the controller with dependencies injected from config.
func NewBackupController(cfg *config.Options) *BackupController {
	return &BackupController{
		Dispatcher:   dispatcher.NewDispatcher(),
		Hasher:       hash.NewDefaultHasher(),
		StoreFactory: func(base string) storage.Storage { return filesystem.NewFileSystemBasedBackup(base) },
		MaxRetries:   cfg.MaxRetries,
		BaseDir:      cfg.BackupRoot,
		GcRetain:     cfg.GcRetain,
	}
}

// Setup wires the backup controller with the manager and starts CRD + backup handlers.
func (bc *BackupController) Setup(ctx context.Context, mgr manager.Manager) error {
	logger := log.FromContext(ctx).WithName("BackupController").WithName("setup")
	logger.Info("setting up backup controller, with options",
		"MaxRetries", bc.MaxRetries,
		"GcRetain", bc.GcRetain,
		"BaseDir", bc.BaseDir)
	// Setup dynamic client and shared informer factory
	dynamicClient := dynamic.NewForConfigOrDie(mgr.GetConfig())
	bc.InformerFactory = dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)

	// Setup CRD client to watch for CRD add/delete events
	apiExtClient, err := apiextensionsclientset.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("failed to create apiextensions client: %w", err)
	}
	// Create and start a shared worker pool for backup processing
	bw := worker.NewBackupWorker("default-backup-worker", bc.Hasher, bc.StoreFactory(bc.BaseDir), 100, bc.MaxRetries, 5)
	bw.StartWorkers(ctx)

	// Launch garbage collector for tombstone cleanup
	garbageCollector := gc.NewGarbageCollector(bc.GcRetain, dynamicClient, bc.StoreFactory(bc.BaseDir))
	go garbageCollector.Run(ctx)

	// Start CRD informer to dynamically track new GVKs
	crdInformerFactory := apiextensionsinformer.NewSharedInformerFactory(apiExtClient, 0)
	crdInformer := crdInformerFactory.Apiextensions().V1().CustomResourceDefinitions().Informer()
	_, err = crdInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			crd := obj.(*apiextensionsv1.CustomResourceDefinition)
			gvk := schema.GroupVersionKind{
				Group:   crd.Spec.Group,
				Version: crd.Spec.Versions[0].Name,
				Kind:    crd.Spec.Names.Kind,
			}
			gvr := schema.GroupVersionResource{
				Group:    crd.Spec.Group,
				Version:  crd.Spec.Versions[0].Name,
				Resource: crd.Spec.Names.Plural,
			}
			// Register informer for new GVK
			_ = bc.Dispatcher.Register(ctx, gvr, gvk, dynamicClient, bw)
			bc.totalRegisteredGVK++
			logger.Info("Registering informers for GVK", "GVK", gvk)
			logger.Info("Current total registered informer per GVK", "count", bc.totalRegisteredGVK)
		},
		DeleteFunc: func(obj interface{}) {
			crd := obj.(*apiextensionsv1.CustomResourceDefinition)
			gvk := schema.GroupVersionKind{
				Group:   crd.Spec.Group,
				Version: crd.Spec.Versions[0].Name,
				Kind:    crd.Spec.Names.Kind,
			}
			bc.totalRegisteredGVK = bc.totalRegisteredGVK - 1
			logger.Info("Deregistering informers for GVK", "GVK", gvk)
			logger.Info("Current total registered informer per GVK", "count", bc.totalRegisteredGVK)
			_ = bc.Dispatcher.Stop(ctx, gvk)
		},
	})
	if err != nil {
		return err
	}
	logger.Info("backup controller setup complete")
	go crdInformerFactory.Start(ctx.Done())
	return nil
}

func (bc *BackupController) FetchDataFromFileServer(group, version, kind string) (map[string]string, error) {

	gvk := schema.GroupVersionKind{
		Group:   group,
		Version: version,
		Kind:    kind,
	}

	return bc.StoreFactory(bc.BaseDir).ReadAllHashes(context.TODO(), gvk)
}
