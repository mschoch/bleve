//  Copyright (c) 2019 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scorch

import (
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/blugelabs/bleve/index"
	"github.com/blugelabs/bleve/index/scorch/segment"
	"github.com/blugelabs/bleve/index/scorch/segment/zap"
)

type segmentWrapperNew func(results []*index.AnalysisResult,
	chunkFactor uint32) (segment.Segment, uint64, error)

type segmentWrapperlOpen func(path string) (segment.Segment, error)

type segmentWrapperMerge func(segments []segment.Segment, drops []*roaring.Bitmap, path string,
	chunkFactor uint32, closeCh chan struct{}, s segment.StatsReporter) (
	[][]uint64, uint64, error)

type segmentWrapperValidateMerge func(segments []segment.Segment, drops []*roaring.Bitmap, newSegment segment.Segment) error

type segmentWrapperIsDocNum1HitFinished func(docNum uint64) bool

type segmentWrapperPostingsIteratorFromBitmap func(bm *roaring.Bitmap,
	includeFreqNorm, includeLocs bool) (segment.PostingsIterator, error)

type segmentWrapperPostingsIteratorFrom1Hit func(docNum1Hit uint64,
	includeFreqNorm, includeLocs bool) (segment.PostingsIterator, error)

type segmentWrapper struct {
	Type                       string
	Version                    uint32
	New                        segmentWrapperNew
	Open                       segmentWrapperlOpen
	Merge                      segmentWrapperMerge
	ValidateMerge              segmentWrapperValidateMerge
	IsDocNum1HitFinished       segmentWrapperIsDocNum1HitFinished
	PostingsIteratorFromBitmap segmentWrapperPostingsIteratorFromBitmap
	PostingsIteratorFrom1Hit   segmentWrapperPostingsIteratorFrom1Hit
}

var supportedSegmentTypeVersions map[string]map[uint32]*segmentWrapper
var defaultSegmentTypeVersion *segmentWrapper

func init() {
	supportedSegmentTypeVersions = map[string]map[uint32]*segmentWrapper{
		zap.Type: map[uint32]*segmentWrapper{
			zap.Version: &segmentWrapper{
				Type:                       zap.Type,
				Version:                    zap.Version,
				New:                        zap.AnalysisResultsToSegmentBase,
				Open:                       zap.Open,
				Merge:                      zap.Merge,
				ValidateMerge:              zap.ValidateMerge,
				IsDocNum1HitFinished:       zap.IsDocNum1HitFinished,
				PostingsIteratorFromBitmap: zap.PostingsIteratorFromBitmap,
				PostingsIteratorFrom1Hit:   zap.PostingsIteratorFrom1Hit,
			},
		},
	}
	defaultSegmentTypeVersion = supportedSegmentTypeVersions[zap.Type][zap.Version]
}

func (s *Scorch) loadSegmentWrapper(segmentType, segmentVersion []byte) error {
	if versions, ok := supportedSegmentTypeVersions[string(segmentType)]; ok {
		version := binary.BigEndian.Uint32(segmentVersion)
		if segWrapper, ok := versions[version]; ok {
			s.segWrapper = segWrapper
			return nil
		}
		return fmt.Errorf("unsupported version %d for segment type: %s, known: %#v", version, string(segmentType), versions)
	}
	return fmt.Errorf("unsupported segment type: %s", string(segmentType))
}
