package opers

import "github.com/spirit-labs/tektite/proc"

func CalcProcessorPartitionMapping(mappingID string, partitionCount int,
	processorCount int) map[int][]int {
	ppm := map[int][]int{}
	for partID := 0; partID < partitionCount; partID++ {
		processorID := proc.CalcProcessorForPartition(mappingID, partID, processorCount)
		ppm[processorID] = append(ppm[processorID], partID)
	}
	return ppm
}

func CalcPartitionProcessorMapping(mappingID string, partitionCount int,
	processorCount int) map[int]int {
	ppm := map[int]int{}
	for partID := 0; partID < partitionCount; partID++ {
		processorID := proc.CalcProcessorForPartition(mappingID, partID, processorCount)
		ppm[partID] = processorID
	}
	return ppm
}
