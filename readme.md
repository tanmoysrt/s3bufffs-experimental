# First Version

We will provide the details, and it's a dummy mount

- Url of file
- What's the filename should be
- uid of file
- gid of file

- On mount only cache the inode info, no change after that
- No need to cache the file content, just serve as per demand

# Second Version

- Chunk Size (8MB)
  - At a time it will download 8MB of data and cache it
- Cache initial and last 1MB of data (mostly metadata)
