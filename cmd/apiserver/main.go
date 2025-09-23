package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"strings"

	"github.com/bastion/internal/config"
	"github.com/bastion/internal/controllers"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	setupLog   = ctrl.Log.WithName("apiserver")
	controller *controllers.BackupController
)

func main() {
	var backupRoot string
	flag.StringVar(&backupRoot, "backup-root", "/backups", "Backup root directory")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	cfg := &config.Options{
		BackupRoot: backupRoot,
	}

	controller = controllers.NewBackupController(cfg)

	http.HandleFunc("/apis/demobastion.io/v1beta1", handler)
	http.HandleFunc("/apis/demobastion.io/v1beta1/", handler)

	setupLog.Info("Starting Custom API Server on 8443")
	err := http.ListenAndServeTLS(":8443", "/certs/tls.crt", "/certs/tls.key", nil)
	if err != nil {
		setupLog.Error(err, "Failed to start HTTPS server")
		os.Exit(1)
	}

}

func handler(w http.ResponseWriter, r *http.Request) {

	gvk := r.URL.Query().Get("gvk") // Capture GVK from request

	if gvk == "" {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"message": "GVK not provided, returning default data"}`))
		return
	}

	parts := strings.Split(gvk, "/")
	if len(parts) != 3 {
		http.Error(w, "Invalid GVK format", http.StatusBadRequest)
		return
	}

	group, version, kind := parts[0], parts[1], parts[2]

	if r.Method == http.MethodGet {
		resources, err := controller.FetchDataFromFileServer(group, version, kind)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		response, err := json.Marshal(resources)
		if err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(response)
	}
}
