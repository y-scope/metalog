package node

import (
	"github.com/y-scope/metalog/node/shared"
)

// SharedResources is an alias for shared.Resources so existing code in the
// node package (and callers) continues to compile without changes.
type SharedResources = shared.Resources
