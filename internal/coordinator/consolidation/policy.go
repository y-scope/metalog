package consolidation

import "github.com/y-scope/metalog/internal/metastore"

// Policy determines which files should be consolidated together.
type Policy interface {
	// SelectFiles returns groups of file records to consolidate.
	// Each group becomes a single consolidation task.
	SelectFiles(candidates []*metastore.FileRecord) [][]*metastore.FileRecord
}
