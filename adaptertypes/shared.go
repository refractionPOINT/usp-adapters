package adaptertypes

import (
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

// Re-export common types used across all adapters
// This keeps the adaptertypes package self-contained while avoiding duplication

type ClientOptions = uspclient.ClientOptions
type Identity = uspclient.Identity
type AckBufferOptions = uspclient.AckBufferOptions
type MappingDescriptor = protocol.MappingDescriptor
type IndexDescriptor = protocol.IndexDescriptor
type FieldMapping = protocol.FieldMapping
