//go:build aix

package usp_sqs_files

import (
	"context"
	"github.com/refractionPOINT/usp-adapters/utils"
)

func parquetToJSONLines(ctx context.Context, data []byte) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	return utils.ParquetToJSONLines(data)
}
