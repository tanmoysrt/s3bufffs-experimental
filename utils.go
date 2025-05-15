package main

import (
	"fmt"
	"io"
	"net/http"
)

func FetchSize(url string) (uint64, error) {
	// Create a new GET request with Range header set to "bytes=0-0"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", "bytes=0-0")

	// Perform the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// Check if the response status code is 206 (Partial Content)
	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("expected status 206, got %d", resp.StatusCode)
	}

	// Get the Content-Range header
	contentRange := resp.Header.Get("Content-Range")
	if contentRange == "" {
		return 0, fmt.Errorf("Content-Range header not found")
	}

	// Parse the Content-Range header, e.g., "bytes 0-0/1289138071"
	var start, end, size uint64
	_, err = fmt.Sscanf(contentRange, "bytes %d-%d/%d", &start, &end, &size)
	if err != nil {
		return 0, fmt.Errorf("failed to parse Content-Range: %v", err)
	}
	return size, nil
}

func FetchFile(url string, offset int64, size int64) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
	req.Header.Set("Range", rangeHeader)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("expected status 206, got %d", resp.StatusCode)
	}

	data := make([]byte, size)
	n, err := io.ReadFull(resp.Body, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read full data: %w (got %d bytes, want %d)", err, n, size)
	}

	return data[:n], nil
}
