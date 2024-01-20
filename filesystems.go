package main

import (
	"golang.org/x/sys/unix"
)

var filesystems = map[int64]string{ //nolint:gochecknoglobals
	unix.EXT4_SUPER_MAGIC: "ext4",
	unix.XFS_SUPER_MAGIC:  "xfs",
}
