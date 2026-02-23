package kubernetescollector

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"gopkg.in/yaml.v2"
)

// loadKubeAPIConfig loads the Kubernetes API client configuration.
//
// This function attempts to load configuration in order:
//  1. In-cluster config (when running as a pod in Kubernetes)
//  2. Local kubeconfig file (when running locally for development)
//
// The second return value indicates whether we're running locally (true)
// or in-cluster (false). This affects how we determine the current node name.
func loadKubeAPIConfig() (*kubeAPIConfig, bool, error) {
	// Try in-cluster config first.
	cfg, inClusterErr := loadInClusterConfig()
	if inClusterErr == nil {
		return cfg, false, nil
	}

	// Fall back to local kubeconfig.
	cfg, localErr := loadLocalConfig()
	if localErr != nil {
		return nil, false, fmt.Errorf("cannot load discovery config from in-cluster config: %w; and from local config: %w", inClusterErr, localErr)
	}
	return cfg, true, nil
}

// loadInClusterConfig loads Kubernetes API configuration from within a pod.
//
// When running in a Kubernetes pod, the following are automatically available:
//   - Service account token: /var/run/secrets/kubernetes.io/serviceaccount/token
//   - CA certificate: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
//   - API server host/port: KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT env vars
//
// This is the production configuration path when vlagent runs as a DaemonSet.
func loadInClusterConfig() (*kubeAPIConfig, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if len(host) == 0 || len(port) == 0 {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST/KUBERNETES_SERVICE_PORT environment variables are not set")
	}

	// Verify that vlagent is running in a Kubernetes cluster by checking
	// for the service account token file.
	const bearerTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	if _, err := os.Stat(bearerTokenFile); err != nil {
		return nil, err
	}

	// Build authentication configuration using the service account.
	opts := &promauth.Options{
		BearerTokenFile: bearerTokenFile,
		TLSConfig: &promauth.TLSConfig{
			CAFile: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		},
	}
	ac, err := opts.NewConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot initialize in-cluster auth config: %w", err)
	}

	// Construct the API server URL.
	server := "https://" + net.JoinHostPort(host, port)
	return &kubeAPIConfig{
		server: server,
		ac:     ac,
	}, nil
}

// kubeConfig represents the structure of a kubeconfig file (~/.kube/config).
// This is a minimal representation containing only the fields we need.
type kubeConfig struct {
	// Clusters contains the cluster definitions.
	Clusters []kubeConfigCluster `yaml:"clusters"`

	// Users contains the user credential definitions.
	Users []kubeConfigUser `yaml:"users"`

	// Contexts contains the context definitions (cluster + user pairs).
	Contexts []kubeConfigContext `yaml:"contexts"`

	// CurrentContext is the name of the context to use.
	CurrentContext string `yaml:"current-context"`
}

// findUser returns the user with the given name.
func (c *kubeConfig) findUser(name string) (kubeConfigUser, bool) {
	for _, u := range c.Users {
		if u.Name == name {
			return u, true
		}
	}
	return kubeConfigUser{}, false
}

// findContext returns the context with the given name.
func (c *kubeConfig) findContext(context string) (kubeConfigContext, bool) {
	for _, c := range c.Contexts {
		if c.Name == context {
			return c, true
		}
	}
	return kubeConfigContext{}, false
}

// findCluster returns the cluster with the given name.
func (c *kubeConfig) findCluster(cluster string) (kubeConfigCluster, bool) {
	for _, cl := range c.Clusters {
		if cl.Name == cluster {
			return cl, true
		}
	}
	return kubeConfigCluster{}, false
}

// kubeConfigCluster represents a cluster entry in kubeconfig.
type kubeConfigCluster struct {
	Name    string `yaml:"name"`
	Cluster struct {
		// Server is the API server URL.
		Server string `yaml:"server"`
		// CertificateAuthority is the path to the CA certificate file.
		CertificateAuthority string `yaml:"certificate-authority"`
		// CertificateAuthorityData is base64-encoded CA certificate data.
		CertificateAuthorityData string `yaml:"certificate-authority-data"`
	} `yaml:"cluster"`
}

// kubeConfigUser represents a user entry in kubeconfig.
type kubeConfigUser struct {
	Name string `yaml:"name"`
	User struct {
		// Token is a bearer token for authentication.
		Token string `yaml:"token"`
		// ClientCertificate is the path to the client certificate file.
		ClientCertificate string `yaml:"client-certificate"`
		// ClientCertificateData is base64-encoded client certificate data.
		ClientCertificateData string `yaml:"client-certificate-data"`
		// ClientKey is the path to the client key file.
		ClientKey string `yaml:"client-key"`
		// ClientKeyData is base64-encoded client key data.
		ClientKeyData string `yaml:"client-key-data"`
	} `yaml:"user"`
}

// kubeConfigContext represents a context entry in kubeconfig.
type kubeConfigContext struct {
	Name    string `yaml:"name"`
	Context struct {
		// Cluster is the name of the cluster to use.
		Cluster string `yaml:"cluster"`
		// User is the name of the user credentials to use.
		User string `yaml:"user"`
	} `yaml:"context"`
}

// loadLocalConfig loads Kubernetes API configuration from a local kubeconfig file.
//
// This is used when running vlagent locally (outside a Kubernetes cluster)
// for development or testing purposes.
//
// The kubeconfig file location is determined by:
//  1. KUBECONFIG environment variable (to match kubectl behavior)
//  2. ~/.kube/config if KUBECONFIG is not set
//
// This function parses the kubeconfig YAML and extracts:
//   - The current context
//   - The cluster server URL and CA certificate
//   - The user credentials (token or client certificate)
func loadLocalConfig() (*kubeAPIConfig, error) {
	// Determine kubeconfig path.
	configPath := os.Getenv("KUBECONFIG")
	if configPath == "" {
		configPath = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}

	// Read and parse the kubeconfig file.
	rawConfig, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var cfg kubeConfig
	if err := yaml.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("cannot parse yaml %q: %w", configPath, err)
	}

	// Find the current context.
	cctx, ok := cfg.findContext(cfg.CurrentContext)
	if !ok {
		return nil, fmt.Errorf("cannot find current context %q in %q", cfg.CurrentContext, configPath)
	}

	// Find the cluster for this context.
	cl, ok := cfg.findCluster(cctx.Context.Cluster)
	if !ok {
		return nil, fmt.Errorf("cannot find cluster %q in %q", cctx.Context.Cluster, configPath)
	}

	// Build TLS configuration from cluster CA.
	tlsCfg := promauth.TLSConfig{}

	if cl.Cluster.CertificateAuthority != "" {
		tlsCfg.CAFile = cl.Cluster.CertificateAuthority
	} else if cl.Cluster.CertificateAuthorityData != "" {
		// Decode base64-encoded CA data.
		ca, err := base64.StdEncoding.DecodeString(cl.Cluster.CertificateAuthorityData)
		if err != nil {
			return nil, fmt.Errorf("cannot decode base64 encoded CA certificate data from file %q: %w", configPath, err)
		}
		tlsCfg.CA = string(ca)
	}

	// Find the user credentials for this context.
	u, ok := cfg.findUser(cctx.Context.User)
	if !ok {
		return nil, fmt.Errorf("cannot find current user %q in %q", cctx.Context.User, configPath)
	}

	// Extract client certificate if present.
	if u.User.ClientCertificate != "" {
		tlsCfg.CertFile = u.User.ClientCertificate
	} else if u.User.ClientCertificateData != "" {
		// Decode base64-encoded client certificate data.
		clientCert, err := base64.StdEncoding.DecodeString(u.User.ClientCertificateData)
		if err != nil {
			return nil, fmt.Errorf("cannot decode base64 encoded client certificate data from file %q: %w", configPath, err)
		}
		tlsCfg.Cert = string(clientCert)
	}

	// Extract client key if present.
	if u.User.ClientKey != "" {
		tlsCfg.KeyFile = u.User.ClientKey
	} else if u.User.ClientKeyData != "" {
		// Decode base64-encoded client key data.
		clientCertKey, err := base64.StdEncoding.DecodeString(u.User.ClientKeyData)
		if err != nil {
			return nil, fmt.Errorf("cannot decode base64 encoded client certificate key data from file %q: %w", configPath, err)
		}
		tlsCfg.Key = string(clientCertKey)
	}

	// Build the authentication configuration.
	opts := &promauth.Options{
		BearerToken: u.User.Token,
		TLSConfig:   &tlsCfg,
	}
	ac, err := opts.NewConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot initialize local auth config from file %q: %w", configPath, err)
	}

	return &kubeAPIConfig{
		server: cl.Cluster.Server,
		ac:     ac,
	}, nil
}
