// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

var _ Index = (*IndexImpl)(nil)

type IndexImpl struct {
	index engine.IndexEngine
	db    Database
	log   log.Logger
}

func (t IndexImpl) DB() Database {
	return t.db
}

func (t IndexImpl) Schema() *schema.Schema {
	return t.index.Schema()
}

func (t IndexImpl) Stats() IndexStats {
	return t.index.Stats()
}

func (t IndexImpl) Engine() engine.IndexEngine {
	return t.index
}
