package bakemono

func (v *Vol) dirProbe(key uint16, segmentId segId, bucketId offset) (hit bool, dirOffset offset) {
	index := bucketId * DirDepth

	for index != 0 {
		if v.Dirs[segmentId][index].offset() == 0 {
			return false, index
		}
		dirKey := v.Dirs[segmentId][index].tag()
		if dirKey == key {
			return true, index
		}
		index = offset(v.Dirs[segmentId][index].next())
	}

	return false, index
}

func (v *Vol) dirClear(segmentId segId, dirOffset offset) {
	v.Dirs[segmentId][dirOffset].clear()
}
