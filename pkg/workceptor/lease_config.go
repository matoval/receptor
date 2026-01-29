//go:build !no_workceptor
// +build !no_workceptor

package workceptor

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/ansible/receptor/pkg/netceptor"
	"github.com/ghjm/cmdline"
	"github.com/spf13/viper"
)

// LeaseServiceCfg is the cmdline configuration object for a lease service.
type LeaseServiceCfg struct {
	Service              string `required:"true" description:"Receptor service name to listen on"`
	TLS                  string `description:"Name of TLS server config for the Receptor listener"`
	Pool                 string `description:"Execution node pool name for grouping workers" default:""`
	MaxRunningJobs       int    `description:"Maximum number of concurrent running jobs" default:"10"`
	MaxOutstandingLeases int    `description:"Maximum number of outstanding leases (0 = same as max_running_jobs)" default:"0"`
	LeaseTTLMs          int    `description:"Default lease TTL in milliseconds" default:"10000"`
	Draining            bool   `description:"Whether to start in draining mode (not accepting new leases)" default:"false"`
}

// Run starts the lease service.
func (cfg LeaseServiceCfg) Run() error {
	ctx := context.Background()

	logger := netceptor.MainInstance.GetLogger()
	logger.Info("Starting lease service with config: %+v", cfg)

	// Get TLS configuration if specified
	var tlsCfg *tls.Config
	var err error
	if cfg.TLS != "" {
		tlsCfg, err = netceptor.MainInstance.GetServerTLSConfig(cfg.TLS)
		if err != nil {
			return fmt.Errorf("error getting TLS config '%s': %s", cfg.TLS, err)
		}
	}

	// Create lease manager configuration
	leaseManagerConfig := &LeaseManagerConfig{
		MaxRunningJobs:       cfg.MaxRunningJobs,
		MaxOutstandingLeases: cfg.MaxOutstandingLeases,
		LeaseTTLMs:          cfg.LeaseTTLMs,
		Draining:            cfg.Draining,
	}

	// Create lease manager
	leaseManager := NewLeaseManager(leaseManagerConfig, logger)

	// Integrate lease manager with main workceptor instance
	if MainInstance != nil {
		MainInstance.SetLeaseManager(leaseManager)
		logger.Info("Integrated lease manager with workceptor main instance")
	} else {
		logger.Warning("No workceptor main instance available for lease integration")
	}

	// Start the lease service
	err = LeaseService(ctx, netceptor.MainInstance, cfg.Service, tlsCfg, cfg.Pool, leaseManager)
	if err != nil {
		return fmt.Errorf("error starting lease service: %s", err)
	}

	return nil
}

func init() {
	version := viper.GetInt("version")
	if version > 1 {
		return
	}
	cmdline.RegisterConfigTypeForApp("receptor-workers",
		"lease-service", "Run a lease service for pull-based job scheduling", LeaseServiceCfg{}, cmdline.Section(workersSection))
}