package common

func MergeStringMaps(map1, map2 map[string]string) map[string]string {
	// Create a new map to store the merged result
	mergedMap := make(map[string]string)

	// Add all entries from map1 to mergedMap
	for k, v := range map1 {
		mergedMap[k] = v
	}

	// Add all entries from map2 to mergedMap
	for k, v := range map2 {
		mergedMap[k] = v
	}

	return mergedMap
}
