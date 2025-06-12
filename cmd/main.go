/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	// +kubebuilder:scaffold:imports
	"crypto/tls"
	"flag"
	"os"
	"path/filepath"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	vitistackcrdsv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/api/controllers"
	"github.com/vitistack/talos-operator/internal/k8sclient"
	"github.com/vitistack/talos-operator/internal/services/initializationservice"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(vitistackcrdsv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// Flags holds all command-line flags for the application
type Flags struct {
	MetricsAddr          string
	MetricsCertPath      string
	MetricsCertName      string
	MetricsCertKey       string
	WebhookCertPath      string
	WebhookCertName      string
	WebhookCertKey       string
	ProbeAddr            string
	EnableLeaderElection bool
	SecureMetrics        bool
	EnableHTTP2          bool
}

func main() {
	// Parse command-line flags
	flags := parseFlags()

	k8sclient.Init()
	// Initialization checks
	initializationservice.CheckPrerequisites()

	// Configure TLS options
	tlsOpts := configureTLS(flags.EnableHTTP2)

	// Set up webhook server with certificate watching if enabled
	webhookServer, webhookCertWatcher := setupWebhookServer(flags, tlsOpts)

	// Set up metrics server with certificate watching if enabled
	metricsOpts, metricsCertWatcher := setupMetricsServer(flags, tlsOpts)

	// Create and configure the controller manager
	mgr := setupManager(flags, &metricsOpts, webhookServer)

	// +kubebuilder:scaffold:builder

	// Add certificate watchers to the manager if they exist
	if metricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "unable to add metrics certificate watcher to manager")
			os.Exit(1)
		}
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	setupReconcilers(mgr, metricsCertWatcher, webhookCertWatcher)

	// Add health and readiness checks
	setupHealthChecks(mgr)

	// Start the manager
	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// parseFlags parses command-line flags and returns them in a Flags struct
func parseFlags() *Flags {
	flags := &Flags{}

	flag.StringVar(&flags.MetricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&flags.ProbeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&flags.EnableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&flags.SecureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&flags.WebhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&flags.WebhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&flags.WebhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&flags.MetricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&flags.MetricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&flags.MetricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&flags.EnableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	// Configure the logger
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	return flags
}

// configureTLS returns TLS configuration options based on HTTP/2 settings
func configureTLS(enableHTTP2 bool) []func(*tls.Config) {
	var tlsOpts []func(*tls.Config)

	// If HTTP/2 is disabled, add a function to disable it in TLS config
	// This prevents vulnerabilities like HTTP/2 Stream Cancellation and Rapid Reset CVEs
	if !enableHTTP2 {
		disableHTTP2 := func(c *tls.Config) {
			setupLog.Info("disabling http/2")
			c.NextProtos = []string{"http/1.1"}
		}
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	return tlsOpts
}

// setupWebhookServer configures the webhook server with certificate watching if provided
func setupWebhookServer(flags *Flags, tlsOpts []func(*tls.Config)) (webhook.Server, *certwatcher.CertWatcher) {
	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	var webhookCertWatcher *certwatcher.CertWatcher

	// Set up certificate watcher if path is provided
	if len(flags.WebhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", flags.WebhookCertPath,
			"webhook-cert-name", flags.WebhookCertName,
			"webhook-cert-key", flags.WebhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(flags.WebhookCertPath, flags.WebhookCertName),
			filepath.Join(flags.WebhookCertPath, flags.WebhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	// Create and return the webhook server
	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	return webhookServer, webhookCertWatcher
}

// setupMetricsServer configures the metrics server with TLS options and certificate watching if provided
func setupMetricsServer(flags *Flags, tlsOpts []func(*tls.Config)) (metricsserver.Options, *certwatcher.CertWatcher) {
	var metricsCertWatcher *certwatcher.CertWatcher

	// Configure metrics server options
	metricsServerOptions := metricsserver.Options{
		BindAddress:   flags.MetricsAddr,
		SecureServing: flags.SecureMetrics,
		TLSOpts:       tlsOpts,
	}

	// Add authorization if secure metrics are enabled
	if flags.SecureMetrics {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// Set up certificate watcher if path is provided
	if len(flags.MetricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", flags.MetricsCertPath,
			"metrics-cert-name", flags.MetricsCertName,
			"metrics-cert-key", flags.MetricsCertKey)

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(flags.MetricsCertPath, flags.MetricsCertName),
			filepath.Join(flags.MetricsCertPath, flags.MetricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "to initialize metrics certificate watcher", "error", err)
			os.Exit(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	return metricsServerOptions, metricsCertWatcher
}

// setupManager creates and configures the controller manager
func setupManager(flags *Flags, metricsOpts *metricsserver.Options, webhookServer webhook.Server) ctrl.Manager {
	// Create manager with provided options
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                *metricsOpts,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: flags.ProbeAddr,
		LeaderElection:         flags.EnableLeaderElection,
		LeaderElectionID:       "6f93c133.vitistack.io",
		// LeaderElectionReleaseOnCancel: true, // Commented out as in original
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	return mgr
}

// setupHealthChecks adds health and readiness checks to the manager
func setupHealthChecks(mgr ctrl.Manager) {
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}
}

func setupReconcilers(mgr ctrl.Manager, metricsCertWatcher *certwatcher.CertWatcher, webhookCertWatcher *certwatcher.CertWatcher) {
	// Add reconcilers here
	// Example:
	// if err := (&controllers.MyReconciler{
	// 	Client: mgr.GetClient(),
	// 	Scheme: mgr.GetScheme(),
	// }).SetupWithManager(mgr); err != nil {
	// 	setupLog.Error(err, "unable to create controller", "controller", "MyReconciler")
	// 	os.Exit(1)
	// }

	// +kubebuilder:scaffold:builder

	setupLog.Info("All controllers and webhooks are set up")
	if err := (&controllers.KubernetesClusterReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "KubernetesClusterReconciler")
		os.Exit(1)
	}
}
