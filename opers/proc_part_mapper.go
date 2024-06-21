// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opers

import "github.com/spirit-labs/tektite/proc"

func CalcProcessorPartitionMapping(mappingID string, partitionCount int,
	processorCount int) map[int][]int {
	ppm := map[int][]int{}
	for partID := 0; partID < partitionCount; partID++ {
		processorID := proc.CalcProcessorForPartition(mappingID, uint64(partID), processorCount)
		ppm[processorID] = append(ppm[processorID], partID)
	}
	return ppm
}

func CalcPartitionProcessorMapping(mappingID string, partitionCount int,
	processorCount int) map[int]int {
	ppm := map[int]int{}
	for partID := 0; partID < partitionCount; partID++ {
		processorID := proc.CalcProcessorForPartition(mappingID, uint64(partID), processorCount)
		ppm[partID] = processorID
	}
	return ppm
}
