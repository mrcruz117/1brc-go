// r2: implimenting go routines to run the solution in parallel
// 10m rows in ~482.45ms
package main

import (
	"bufio"
	// "fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type stats struct {
	min, max, sum float64
	count         int64
}

// var maxGoroutines int = 4 // Adjust this based on your machine's capabilities

func r2(inputPath string, output io.Writer) error {
	// Open the input file
	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Prepare to read the file
	scanner := bufio.NewScanner(f)

	// Variables for chunk processing
	var (
		chunkSize = 150000 // Adjust based on memory and performance
		lines     []string
		chunkList [][]string
	)

	// Read the file and divide it into chunks
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) >= chunkSize {
			chunkList = append(chunkList, lines)
			lines = nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// Add any remaining lines as the last chunk
	if len(lines) > 0 {
		chunkList = append(chunkList, lines)
	}

	// Channel to collect results from goroutines
	results := make(chan map[string]stats, len(chunkList))

	// Limit the number of concurrent goroutines

	semaphore := make(chan struct{}, maxGoroutines)

	var wg sync.WaitGroup
	for _, chunk := range chunkList {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire a token

		// Process each chunk in a goroutine
		go func(lines []string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release the token

			localStats := processChunk(lines)
			results <- localStats
		}(chunk)
	}

	// Close the results channel when all goroutines are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Aggregate results from all goroutines
	stationStats := make(map[string]stats)
	for localStats := range results {
		for station, s := range localStats {
			if aggStat, exists := stationStats[station]; exists {
				aggStat.min = min(aggStat.min, s.min)
				aggStat.max = max(aggStat.max, s.max)
				aggStat.sum += s.sum
				aggStat.count += s.count
				stationStats[station] = aggStat
			} else {
				stationStats[station] = s
			}
		}
	}

	// Output the results
	stations := make([]string, 0, len(stationStats))
	for station := range stationStats {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	// fmt.Fprint(output, "{")
	// for i, station := range stations {
	// 	if i > 0 {
	// 		fmt.Fprint(output, ", ")
	// 	}
	// 	s := stationStats[station]
	// 	mean := s.sum / float64(s.count)
	// 	fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, s.min, mean, s.max)
	// }
	//  fmt.Fprint(output, "}\n")
	return nil
}

// Process a chunk of lines and return local statistics
func processChunk(lines []string) map[string]stats {
	localStats := make(map[string]stats)
	for _, line := range lines {
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}
		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			continue
		}
		s, exists := localStats[station]
		if !exists {
			s = stats{min: temp, max: temp, sum: temp, count: 1}
		} else {
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++
		}
		localStats[station] = s
	}
	return localStats
}
