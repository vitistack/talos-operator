package validationservice

import (
	"fmt"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

// ValidationService provides validation operations for Kubernetes resources
type ValidationService struct{}

// NewValidationService creates a new ValidationService
func NewValidationService() *ValidationService {
	return &ValidationService{}
}

// ValidationError represents a validation error with a field path
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	if len(e) == 1 {
		return e[0].Error()
	}
	msg := "multiple validation errors:"
	for _, err := range e {
		msg += "\n  - " + err.Error()
	}
	return msg
}

// ValidateKubernetesCluster validates a KubernetesCluster resource
// Returns nil if valid, or ValidationErrors if there are issues
func (s *ValidationService) ValidateKubernetesCluster(cluster *vitistackv1alpha1.KubernetesCluster) error {
	var errs ValidationErrors

	// Validate control plane replicas (must be odd for etcd quorum)
	if err := validateControlPlaneReplicas(cluster); err != nil {
		errs = append(errs, *err)
	}

	// Validate node pools - warn if no node pools are defined
	if err := validateNodePools(cluster); err != nil {
		errs = append(errs, *err)
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

// validateControlPlaneReplicas ensures control plane replicas is an odd number
// for proper etcd quorum (1, 3, 5, etc.)
func validateControlPlaneReplicas(cluster *vitistackv1alpha1.KubernetesCluster) *ValidationError {
	replicas := cluster.Spec.Topology.ControlPlane.Replicas

	// Must be at least 1
	if replicas < 1 {
		return &ValidationError{
			Field:   "spec.topology.controlPlane.replicas",
			Message: fmt.Sprintf("must be at least 1, got %d", replicas),
		}
	}

	// Must be odd for etcd quorum
	if replicas%2 == 0 {
		return &ValidationError{
			Field:   "spec.topology.controlPlane.replicas",
			Message: fmt.Sprintf("must be an odd number (1, 3, 5, etc.) to maintain etcd quorum, got %d", replicas),
		}
	}

	return nil
}

// ValidateControlPlaneScaleDown validates that a control plane scale-down operation is safe
// currentCount is the current number of control planes
// targetCount is the desired number after scale-down
func (s *ValidationService) ValidateControlPlaneScaleDown(currentCount, targetCount int) error {
	var errs ValidationErrors

	// Cannot scale to 0
	if targetCount < 1 {
		errs = append(errs, ValidationError{
			Field:   "spec.topology.controlPlane.replicas",
			Message: "cannot scale control planes to 0, minimum is 1",
		})
	}

	// Target must be odd
	if targetCount >= 1 && targetCount%2 == 0 {
		errs = append(errs, ValidationError{
			Field:   "spec.topology.controlPlane.replicas",
			Message: fmt.Sprintf("target replicas must be an odd number (1, 3, 5, etc.) to maintain etcd quorum, got %d", targetCount),
		})
	}

	// Cannot skip quorum transitions (e.g., 5->1 is unsafe, should go 5->3->1)
	if currentCount > 3 && targetCount == 1 {
		errs = append(errs, ValidationError{
			Field:   "spec.topology.controlPlane.replicas",
			Message: fmt.Sprintf("unsafe scale-down from %d to %d; scale down gradually (e.g., 5->3->1) to maintain quorum", currentCount, targetCount),
		})
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

// validateNodePools validates that removing all node pools is not allowed
// Reducing replicas within a node pool is fine, but having zero node pools
// would orphan pods and leave the cluster without worker capacity
func validateNodePools(cluster *vitistackv1alpha1.KubernetesCluster) *ValidationError {
	nodePools := cluster.Spec.Topology.Workers.NodePools

	// Check if there are no node pools defined
	if len(nodePools) == 0 {
		return &ValidationError{
			Field:   "spec.topology.workers.nodePools",
			Message: "at least one node pool is required; removing all node pools would destroy all worker nodes and orphan running pods. To scale down, reduce replicas in existing node pools instead",
		}
	}

	// Calculate total worker replicas across all node pools
	totalReplicas := 0
	for i := range nodePools {
		np := &nodePools[i]
		totalReplicas += np.Replicas
	}

	// Warn if total replicas is zero (all pools have 0 replicas)
	if totalReplicas == 0 {
		return &ValidationError{
			Field:   "spec.topology.workers.nodePools",
			Message: "total worker replicas across all node pools is 0; this will remove all worker nodes and orphan running pods",
		}
	}

	return nil
}
