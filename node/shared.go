package node

import (
	"github.com/y-scope/metalog/node/resources"
)

// SharedResources is an alias for resources.Resources so existing code in the
// node package (and callers) continues to compile without changes.
type SharedResources = resources.Resources
