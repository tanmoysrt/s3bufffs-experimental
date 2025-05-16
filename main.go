package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/jacobsa/fuse"
)

func main() {
	url := "https://tets-tanmoy-fc-bucket.s3.ap-south-1.amazonaws.com/social-network.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAU72LF4HG34FXEOIT%2F20250514%2Fap-south-1%2Fs3%2Faws4_request&X-Amz-Date=20250514T183839Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=3d19389c7ae2ed9d04a24db65a1660b5848a22a8ac4ad98eb7ed124edd1ed789"

	// data, err := FetchFile(url, 0, 65537)
	// if err != nil {
	// 	fmt.Println("Error fetching file:", err)
	// 	return
	// }
	// fmt.Println("Fetched data size:", len(data))
	// fmt.Println("Fetched data:", string(data))
	// return

	server, err := NewS3FSRead([]*S3FileNode{
		{
			Name:              "social-network.mp4",
			Size:              0,
			URL:               url,
			CacheBlockSize:    1024 * 1024, // 1MB
			CacheBlock:        make(map[int64]*CacheBlock),
			CacheBlockRWMutex: sync.RWMutex{},
		},
	})
	if err != nil {
		fmt.Println("Error creating S3FSRead:", err)
		return
	}

	// Try to unmount if it's already mounted.
	_ = fuse.Unmount("/mnt/test")

	// Mount the file system.
	cfg := fuse.MountConfig{
		ReadOnly: true,
		FSName:   "s3readfs",
	}
	// cfg.DebugLogger = log.New(os.Stderr, "fuse: ", 0)

	mfs, err := fuse.Mount("/mnt/test", server, &cfg)
	if err != nil {
		log.Fatalf("failed to mount: %v", err)
	}
	fmt.Println("Mounted successfully")

	// Wait for it to be unmounted.
	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}
	fmt.Println("Unmounted successfully")
}
