package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type BatchJob struct {
	Batch            []map[string]interface{}
	MicroserviceName string
}

func extractTGZ(tgzPath, destDir string) error {
	if _, err := os.Stat(destDir); !os.IsNotExist(err) {
		if err := os.RemoveAll(destDir); err != nil {
			return fmt.Errorf("failed to remove existing extracted_logs directory: %w", err)
		}
	}
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create extracted_logs directory: %w", err)
	}
	file, err := os.Open(tgzPath)
	if err != nil {
		return err
	}
	defer file.Close()
	gzr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		target := filepath.Join(destDir, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			outFile, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				return err
			}
			outFile.Close()
		}
	}
	return nil
}

func extractZIP(zipPath, destDir string) error {
	if _, err := os.Stat(destDir); !os.IsNotExist(err) {
		if err := os.RemoveAll(destDir); err != nil {
			return fmt.Errorf("failed to remove existing extracted_logs directory: %w", err)
		}
	}
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create extracted_logs directory: %w", err)
	}
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		if strings.HasPrefix(f.Name, "__MACOSX/") {
			continue
		}
		fpath := filepath.Join(destDir, f.Name)
		if !strings.HasPrefix(fpath, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path: %s", fpath)
		}
		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}
		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}
	return nil
}

func parseLogFile(filePath string, batchSize int) ([][]map[string]interface{}, error) {
	var allBatches [][]map[string]interface{}
	var batch []map[string]interface{}
	var batchCount int
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var logEntry map[string]interface{}
		err := json.Unmarshal([]byte(scanner.Text()), &logEntry)
		if err != nil {
			fmt.Printf("Skipping invalid JSON line in file %s: %s\n", filePath, err)
			continue
		}
		batch = append(batch, logEntry)
		batchCount++
		if batchCount >= batchSize {
			allBatches = append(allBatches, batch)
			batch = nil
			batchCount = 0
		}
	}
	if len(batch) > 0 {
		allBatches = append(allBatches, batch)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return allBatches, nil
}

func sendLogsToElasticsearch(logs []map[string]interface{}, indexName string) error {
	const esURL = "http://localhost:9200/%s/_bulk"
	url := fmt.Sprintf(esURL, indexName)
	var bulkData strings.Builder
	for _, log := range logs {
		meta := fmt.Sprintf(`{ "index" : {} }%s`, "\n")
		data, err := json.Marshal(log)
		if err != nil {
			return err
		}
		bulkData.WriteString(meta)
		bulkData.Write(data)
		bulkData.WriteString("\n")
	}
	fmt.Printf("Sending %d log entries to Elasticsearch for index %s\n", len(logs), indexName)
	req, err := http.NewRequest("POST", url, strings.NewReader(bulkData.String()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send data to Elasticsearch: %s - %s", resp.Status, body)
	}
	return nil
}

func createKibanaIndexPattern(indexPatternName string) error {
	if indexPatternExists(indexPatternName) {
		fmt.Printf("Index pattern %s already exists in Kibana, skipping creation.\n", indexPatternName)
		return nil
	}
	indexPattern := map[string]interface{}{
		"attributes": map[string]interface{}{
			"title":         indexPatternName,
			"timeFieldName": "time",
		},
	}
	jsonData, err := json.Marshal(indexPattern)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	req, err := http.NewRequest("POST", "http://localhost:5601/api/saved_objects/index-pattern", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("kbn-xsrf", "true")
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create index pattern in Kibana: %s", body)
	}
	return nil
}

func indexPatternExists(indexPatternName string) bool {
	url := fmt.Sprintf("http://localhost:5601/api/saved_objects/_find?type=index-pattern&search_fields=title&search=%s", indexPatternName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Printf("Error creating HTTP request to check index pattern existence: %v\n", err)
		return false
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending HTTP request to check index pattern existence: %v\n", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Error response from Kibana when checking index pattern existence: %s\n", resp.Status)
		return false
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("Error decoding JSON response from Kibana: %v\n", err)
		return false
	}

	savedObjects, ok := result["saved_objects"].([]interface{})
	if !ok {
		fmt.Println("Unexpected format in Kibana response")
		return false
	}

	return len(savedObjects) > 0
}

func extractMicroserviceName(fileName string) string {
	// Remove the file extension and return the file name as the microservice name
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

func worker(id int, jobs <-chan BatchJob, results chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Worker %d: Processing batch for %s\n", id, job.MicroserviceName)
		err := sendLogsToElasticsearch(job.Batch, job.MicroserviceName)
		if err != nil {
			results <- err
			break
		}
		results <- nil
		fmt.Printf("Worker %d: Finished processing batch for %s\n", id, job.MicroserviceName)
	}
}

func main() {
	flag.Parse()
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter the path to the archive file (TGZ or ZIP): ")
	archivePath, _ := reader.ReadString('\n')
	archivePath = strings.TrimSpace(archivePath)
	usr, err := user.Current()
	if err != nil {
		fmt.Println("Failed to get home directory:", err)
		return
	}
	destDir := filepath.Join(usr.HomeDir, "extracted_logs")
	if strings.HasSuffix(archivePath, ".tgz") || strings.HasSuffix(archivePath, ".tar.gz") {
		if err := extractTGZ(archivePath, destDir); err != nil {
			fmt.Println("Failed to extract TGZ:", err)
			return
		}
	} else if strings.HasSuffix(archivePath, ".zip") {
		if err := extractZIP(archivePath, destDir); err != nil {
			fmt.Println("Failed to extract ZIP:", err)
			return
		}
	} else {
		fmt.Println("Unsupported file format. Please provide a .tgz, .tar.gz, or .zip file.")
		return
	}
	var batchJobs []BatchJob
	err = filepath.Walk(destDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".log") {
			microserviceName := extractMicroserviceName(info.Name())
			logBatches, err := parseLogFile(path, 50000)
			if err != nil {
				return err
			}
			for _, batch := range logBatches {
				batchJobs = append(batchJobs, BatchJob{Batch: batch, MicroserviceName: microserviceName})
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println("Error processing log files:", err)
		return
	}
	jobs := make(chan BatchJob, len(batchJobs))
	results := make(chan error, len(batchJobs))
	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() * 1 / 2 // Adjust the number of workers as needed
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, results, &wg)
	}
	for _, job := range batchJobs {
		jobs <- job
	}
	close(jobs)
	go func() {
		wg.Wait()
		close(results)
	}()
	for err := range results {
		if err != nil {
			fmt.Println("Error:", err)
		}
	}
	fmt.Println("Log files processed and sent to Elasticsearch successfully!")
	for _, job := range batchJobs {
		if err := createKibanaIndexPattern(job.MicroserviceName); err != nil {
			fmt.Printf("Error creating index pattern for %s in Kibana: %v\n", job.MicroserviceName, err)
			return
		}
	}
	fmt.Println("Index patterns created in Kibana successfully!")
}
