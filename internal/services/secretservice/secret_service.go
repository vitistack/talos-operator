package secretservice

import (
	"context"
	"fmt"

	"github.com/spf13/viper"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SecretService handles secret operations for Talos clusters
type SecretService struct {
	client.Client
}

// NewSecretService creates a new secret service
func NewSecretService(c client.Client) *SecretService {
	return &SecretService{
		Client: c,
	}
}

// GetSecretName returns the standardized secret name for a cluster
func GetSecretName(cluster *vitistackcrdsv1alpha1.KubernetesCluster) string {
	return fmt.Sprintf("%s%s", viper.GetString(consts.SECRET_PREFIX), cluster.Spec.Cluster.ClusterId)
}

// GetTalosSecret retrieves the Talos secret for a cluster
func (s *SecretService) GetTalosSecret(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) (*corev1.Secret, error) {
	secretName := GetSecretName(cluster)
	secret := &corev1.Secret{}
	err := s.Get(ctx, types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}, secret)
	return secret, err
}

// CreateTalosSecret creates a new Talos secret for a cluster
func (s *SecretService) CreateTalosSecret(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster, data map[string][]byte) error {
	secretName := GetSecretName(cluster)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"cluster.vitistack.io/cluster-id": cluster.Spec.Cluster.ClusterId,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	return s.Create(ctx, secret)
}

// UpdateTalosSecret updates an existing Talos secret for a cluster
func (s *SecretService) UpdateTalosSecret(ctx context.Context, secret *corev1.Secret) error {
	return s.Update(ctx, secret)
}
