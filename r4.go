// r3: add custom hashing
//
// ~5.5s for 1B rows

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// CounterItem is the type returned by Counter.Items.
type CounterItem struct {
	Key   []byte // key, nil in Counter.items if not yet used
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

// Counter is a hash table for counting frequencies and statistics of short strings.
type Counter struct {
	items []CounterItem // hash buckets, linearly probed
	size  int           // number of active items in items slice
}

const (
	// FNV-1 64-bit constants from hash/fnv.
	offset64 = 14695981039346656037
	prime64  = 1099511628211

	// Initial length for Counter.items.
	initialLen = 1024
)

// Update increments or updates the statistics for the given key.
func (c *Counter) Update(key []byte, value float64) {
	hash := uint64(offset64)
	for _, b := range key {
		hash *= prime64
		hash ^= uint64(b)
	}
	index := int(hash & uint64(len(c.items)-1))

	// If the table is more than half full, resize
	if c.size >= len(c.items)/2 {
		newLen := len(c.items) * 2
		if newLen == 0 {
			newLen = initialLen
		}
		newC := Counter{items: make([]CounterItem, newLen)}
		for _, item := range c.items {
			if item.Key != nil {
				newC.InsertItem(item)
			}
		}
		c.items = newC.items
		index = int(hash & uint64(len(c.items)-1))
	}

	// Insert or update
	for {
		if c.items[index].Key == nil {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			c.items[index] = CounterItem{
				Key:   keyCopy,
				Min:   value,
				Max:   value,
				Sum:   value,
				Count: 1,
			}
			c.size++
			return
		}
		if bytes.Equal(c.items[index].Key, key) {
			item := &c.items[index]
			if value < item.Min {
				item.Min = value
			}
			if value > item.Max {
				item.Max = value
			}
			item.Sum += value
			item.Count++
			return
		}
		index++
		if index >= len(c.items) {
			index = 0
		}
	}
}

// InsertItem inserts an item during table resizing
func (c *Counter) InsertItem(item CounterItem) {
	hash := uint64(offset64)
	for _, b := range item.Key {
		hash *= prime64
		hash ^= uint64(b)
	}
	index := int(hash & uint64(len(c.items)-1))

	// Insert without updating counts
	for {
		if c.items[index].Key == nil {
			c.items[index] = item
			c.size++
			return
		}
		index++
		if index >= len(c.items) {
			index = 0
		}
	}
}

// MergeItem merges statistics from another CounterItem into the current Counter
func (c *Counter) MergeItem(newItem CounterItem) {
	hash := uint64(offset64)
	for _, b := range newItem.Key {
		hash *= prime64
		hash ^= uint64(b)
	}
	index := int(hash & uint64(len(c.items)-1))

	// Resize if necessary
	if c.size >= len(c.items)/2 {
		newLen := len(c.items) * 2
		if newLen == 0 {
			newLen = initialLen
		}
		newC := Counter{items: make([]CounterItem, newLen)}
		for _, item := range c.items {
			if item.Key != nil {
				newC.InsertItem(item)
			}
		}
		c.items = newC.items
		index = int(hash & uint64(len(c.items)-1))
	}

	// Merge or insert
	for {
		if c.items[index].Key == nil {
			// New entry
			c.items[index] = newItem
			c.size++
			return
		}
		if bytes.Equal(c.items[index].Key, newItem.Key) {
			item := &c.items[index]
			if newItem.Min < item.Min {
				item.Min = newItem.Min
			}
			if newItem.Max > item.Max {
				item.Max = newItem.Max
			}
			item.Sum += newItem.Sum
			item.Count += newItem.Count
			return
		}
		index++
		if index >= len(c.items) {
			index = 0
		}
	}
}

// Items returns a copy of the incremented items.
func (c *Counter) Items() []CounterItem {
	var items []CounterItem
	for _, item := range c.items {
		if item.Key != nil {
			items = append(items, item)
		}
	}
	return items
}

func r4(inputPath string, output io.Writer) error {
	parts, err := r4splitFile(inputPath, maxGoroutines)
	if err != nil {
		return err
	}

	resultsCh := make(chan *Counter)
	for _, part := range parts {
		go r4ProcessPart(inputPath, part.offset, part.size, resultsCh)
	}

	// Initialize a final Counter to hold aggregated results
	finalCounter := &Counter{}

	// Collect results from goroutines
	for i := 0; i < len(parts); i++ {
		partialCounter := <-resultsCh
		for _, item := range partialCounter.Items() {
			finalCounter.MergeItem(item)
		}
	}

	// Extract and sort the results
	items := finalCounter.Items()
	sort.Slice(items, func(i, j int) bool {
		return bytes.Compare(items[i].Key, items[j].Key) < 0
	})

	// Output the results
	fmt.Fprint(output, "{")
	for i, item := range items {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		mean := item.Sum / float64(item.Count)
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", string(item.Key), item.Min, mean, item.Max)
	}
	fmt.Fprint(output, "}\n")
	fmt.Fprint(output, len(items), " stations\n")

	return nil
}

func r4ProcessPart(inputPath string, fileOffset, fileSize int64, resultsCh chan *Counter) {
	file, err := os.Open(inputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	f := io.LimitedReader{R: file, N: fileSize}

	// Initialize custom Counter
	counter := &Counter{}

	scanner := bufio.NewScanner(&f)
	for scanner.Scan() {
		line := scanner.Text()
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			panic(err)
		}

		// Use custom Counter
		counter.Update([]byte(station), temp)
	}

	resultsCh <- counter
}

func r4splitFile(inputPath string, numParts int) ([]part, error) {
	const maxLineLength = 100

	f, err := os.Open(inputPath)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	splitSize := size / int64(numParts)

	buf := make([]byte, maxLineLength)

	parts := make([]part, 0, numParts)
	offset := int64(0)
	for i := 0; i < numParts; i++ {
		if i == numParts-1 {
			if offset < size {
				parts = append(parts, part{offset, size - offset})
			}
			break
		}

		seekOffset := max(offset+splitSize-maxLineLength, 0)
		_, err := f.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		n, _ := io.ReadFull(f, buf)
		chunk := buf[:n]
		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			return nil, fmt.Errorf("newline not found at offset %d", offset+splitSize-maxLineLength)
		}
		remaining := len(chunk) - newline - 1
		nextOffset := seekOffset + int64(len(chunk)) - int64(remaining)
		parts = append(parts, part{offset, nextOffset - offset})
		offset = nextOffset
	}
	return parts, nil
}
