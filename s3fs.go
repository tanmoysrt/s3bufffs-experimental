package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

/*
Clone Filesystem

- It just stores files in clone folder
- No directory support (only files) [just for simplicity of inode management]
- No sym links (so no CreateLink/Unlink)
- No extra attributes (so no GetXattr/ListXattr/SetXattr/RemoveXattr)
- No fallocate
- File UID/GID set to 1000
- All files are 0777 (rwx)
*/

type S3FSRead struct {
	fuseutil.NotImplementedFileSystem

	InodeMap       map[fuseops.InodeID]*S3FileNode // inode ID -> inode
	FileInodeIdMap map[string]fuseops.InodeID
	Dirents        []fuseutil.Dirent

	nextInodeID fuseops.InodeID
}

type S3FileNode struct {
	Name              string
	Size              uint64
	URL               string
	CacheBlockSize    int64
	CacheBlock        map[int64]*CacheBlock
	CacheBlockRWMutex sync.RWMutex
}

type CacheBlock struct {
	Data      []byte
	Available bool
	Mutex     sync.RWMutex
}

func (f *S3FileNode) Read(offset int64, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}

	var result []byte
	bytesRead := int64(0)
	for bytesRead < size {
		blockOffset := ((offset + bytesRead) / f.CacheBlockSize) * f.CacheBlockSize
		block, err := f.readCacheBlock(blockOffset)
		if err != nil {
			return nil, err
		}

		// Calculate start and end within the block
		startInBlock := (offset + bytesRead) - blockOffset
		bytesLeft := size - bytesRead
		bytesInBlock := f.CacheBlockSize - startInBlock
		toCopy := bytesInBlock
		if bytesLeft < bytesInBlock {
			toCopy = bytesLeft
		}

		result = append(result, block[startInBlock:startInBlock+toCopy]...)
		bytesRead += toCopy
	}

	// Prefetch logic: If more than 30% of the last block has been read, prefetch next 2 blocks
	lastReadOffset := offset + bytesRead - 1
	if lastReadOffset >= 0 {
		lastBlockOffset := (lastReadOffset / f.CacheBlockSize) * f.CacheBlockSize
		startInLastBlock := lastReadOffset - lastBlockOffset + 1
		if startInLastBlock > f.CacheBlockSize/3 {
			go f.readCacheBlock(lastBlockOffset + f.CacheBlockSize)
			go f.readCacheBlock(lastBlockOffset + f.CacheBlockSize*2)
		}
	}

	// Remove old cache blocks
	f.removeCacheBlockBefore(offset)

	return result, nil
}

func (f *S3FileNode) removeCacheBlockBefore(offset int64) {
	// No need to go agressively
	// If there are more than 5 blocks in cache, remove the old ones
	if len(f.CacheBlock) < 5 {
		return
	}

	f.CacheBlockRWMutex.Lock()
	defer f.CacheBlockRWMutex.Unlock()

	for k := range f.CacheBlock {
		if k < offset {
			delete(f.CacheBlock, k)
		}
	}
}

func (f *S3FileNode) readCacheBlock(offset int64) ([]byte, error) {
	// Check if the block mapping available already
	f.CacheBlockRWMutex.RLock()
	block, exists := f.CacheBlock[offset]
	f.CacheBlockRWMutex.RUnlock()
	if exists {
		block.Mutex.RLock()
		defer block.Mutex.RUnlock()
		if block.Available {
			return block.Data, nil
		} else {
			return nil, fmt.Errorf("block not available")
		}
	} else {
		// Block not available, so create a new one
		block = &CacheBlock{
			Data:      make([]byte, f.CacheBlockSize),
			Available: false,
			Mutex:     sync.RWMutex{},
		}
		f.CacheBlockRWMutex.Lock()
		f.CacheBlock[offset] = block
		f.CacheBlockRWMutex.Unlock()

		block.Mutex.Lock()
		defer block.Mutex.Unlock()
		data, err := FetchFile(f.URL, offset, f.CacheBlockSize)
		if err != nil {
			// In case of failure
			// Remove the block from the cache store
			f.CacheBlockRWMutex.Lock()
			delete(f.CacheBlock, offset)
			f.CacheBlockRWMutex.Unlock()
			return nil, err
		}
		block.Data = data
		block.Available = true
		return data, nil
	}
}

func NewS3FSRead(fileNodes []*S3FileNode) (fuse.Server, error) {
	fs := &S3FSRead{
		InodeMap: map[fuseops.InodeID]*S3FileNode{
			fuseops.RootInodeID: {
				Name: ".",
				Size: 0,
			},
		},
		FileInodeIdMap: map[string]fuseops.InodeID{
			".": fuseops.RootInodeID,
		},
		nextInodeID: fuseops.RootInodeID + 1,
		Dirents:     make([]fuseutil.Dirent, 0, len(fileNodes)),
	}

	// Fetch the size of each file and create the inode
	for _, file := range fileNodes {
		size, err := FetchSize(file.URL)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch size for %s: %v", file.Name, err)
		}
		file.Size = size
	}

	// Create Inode & Dirent for each file
	for i, fileNode := range fileNodes {
		// Create Inode
		inodeID := fs.nextInodeID
		fs.FileInodeIdMap[fileNode.Name] = inodeID
		fs.InodeMap[inodeID] = fileNode
		fs.nextInodeID++

		// Create Dirent
		fs.Dirents = append(fs.Dirents, fuseutil.Dirent{
			Offset: fuseops.DirOffset(i + 1),
			Inode:  inodeID,
			Name:   fileNode.Name,
			Type:   fuseutil.DT_File,
		})
	}

	return fuseutil.NewFileSystemServer(fs), nil
}

// Inode Functions

func (fs *S3FSRead) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	if op.Parent != fuseops.RootInodeID {
		return fuse.ENOENT
	}
	// find the inode ID of the file
	inodeID, ok := fs.FileInodeIdMap[op.Name]
	if !ok {
		return fuse.ENOENT
	}
	// find the inode info
	info, ok := fs.InodeMap[inodeID]
	if !ok {
		return fuse.ENOENT
	}
	op.Entry = fuseops.ChildInodeEntry{
		Child: inodeID,
	}
	fs.setDefaultExtraAttributes(&op.Entry.Attributes)
	op.Entry.Attributes.Size = info.Size
	return nil
}

func (fs *S3FSRead) setDefaultExtraAttributes(attr *fuseops.InodeAttributes) {
	attr.Nlink = 1
	attr.Mode = 0777
	attr.Uid = 1000
	attr.Gid = 1000
	attr.Atime = time.Now()
	attr.Ctime = time.Now()
	attr.Mtime = time.Now()
}

func (fs *S3FSRead) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	// Handle Root Inode specially
	if op.Inode == fuseops.RootInodeID {
		fs.setDefaultExtraAttributes(&op.Attributes)
		op.Attributes.Mode = os.ModeDir | 0777
		return nil
	}

	inode, ok := fs.InodeMap[op.Inode]
	if !ok {
		return fuse.ENOENT
	}

	op.Attributes.Size = inode.Size

	// Some default values
	fs.setDefaultExtraAttributes(&op.Attributes)
	return nil
}

func (fs *S3FSRead) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) error {
	if op.Inode != fuseops.RootInodeID {
		return nil
	}

	if op.Offset > fuseops.DirOffset(len(fs.Dirents)) {
		return nil
	}

	entries := fs.Dirents[op.Offset:]

	for _, entry := range entries {
		i := fuseutil.WriteDirent(op.Dst[op.BytesRead:], entry)
		if i == 0 {
			fmt.Println("Buffer too small")
			// 0 means that the buffer was too small and cannot write dirent
			break
		}
		op.BytesRead += i
	}

	return nil
}

func (fs *S3FSRead) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	inode, ok := fs.InodeMap[op.Inode]
	if !ok {
		return fuse.ENOENT
	}

	data, err := inode.Read(op.Offset, op.Size)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return err
	}

	copy(op.Dst, data)
	op.BytesRead = len(data)
	return nil
}

// Mostly Dummy Function to satisfy the interface
// Some are not implemented as they are not needed

func (fs *S3FSRead) ForgetInode(ctx context.Context, op *fuseops.ForgetInodeOp) error { return nil }

func (fs *S3FSRead) BatchForget(ctx context.Context, op *fuseops.BatchForgetOp) error { return nil }

// Directory Related - Mostly Dummy

func (fs *S3FSRead) OpenDir(ctx context.Context, op *fuseops.OpenDirOp) error {
	if op.Inode == fuseops.RootInodeID {
		return nil
	}
	return fuse.ENOENT
}

func (fs *S3FSRead) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) error {
	return nil
}

// File Management

func (fs *S3FSRead) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error { return nil }

func (fs *S3FSRead) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error { return nil }

func (fs *S3FSRead) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) error { return nil }

func (fs *S3FSRead) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) error {
	return nil
}

// File System Functions
func (fs *S3FSRead) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	// OS X specific
	return nil
}

func (fs *S3FSRead) SyncFS(ctx context.Context, op *fuseops.SyncFSOp) error { return nil }
