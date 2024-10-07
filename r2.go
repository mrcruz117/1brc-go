package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"strings"
	"sync"
)

type Stats struct {
	Total   int
	Success int
	Failure int
}

func processChunk(lines []string, results chan<- Stats, wg *sync.WaitGroup, sem chan struct{}) {
	defer wg.Done()
	defer func() { <-sem }() // Ensure semaphore is released

	var stats Stats
	for _, line := range lines {
		stats.Total++
		if strings.Contains(line, "success") {
			stats.Success++
		} else if strings.Contains(line, "failure") {
			stats.Failure++
		}
	}
	results <- stats
}

func mergeStats(results <-chan Stats, finalStats *Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	for stats := range results {
		finalStats.Total += stats.Total
		finalStats.Success += stats.Success
		finalStats.Failure += stats.Failure
	}
}

func r2(inputPath string, output io.Writer) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	chunkSize := (len(lines) + maxGoroutines - 1) / maxGoroutines
	results := make(chan Stats, maxGoroutines)
	var finalStats Stats

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxGoroutines)

	for i := 0; i < len(lines); i += chunkSize {
		end := i + chunkSize
		if end > len(lines) {
			end = len(lines)
		}
		wg.Add(1)
		sem <- struct{}{}
		go processChunk(lines[i:end], results, &wg, sem)
	}

	wg.Add(1)
	go mergeStats(results, &finalStats, &wg)

	wg.Wait()
	close(results)

	fmt.Fprintf(output, "Total: %d\n", finalStats.Total)
	fmt.Fprintf(output, "Success: %d\n", finalStats.Success)
	fmt.Fprintf(output, "Failure: %d\n", finalStats.Failure)

	return nil
}
