package schema

import (
	"fmt"

	"github.com/y-scope/metalog/internal/metastore"
)

// ValidateStateTransition returns an error if the transition is not valid.
func ValidateStateTransition(from, to metastore.FileState) error {
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("invalid state transition: %s -> %s", from, to)
	}
	return nil
}
