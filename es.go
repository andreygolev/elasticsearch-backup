package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type ESResponse struct {
	Snapshots []Snapshot `json:"snapshots"`
}

type Snapshot struct {
	Snapshot string `json:"snapshot"`
	State    string `json:"state"`
}

type ESAckStatus struct {
	Acknowledged bool `json:"acknowledged"`
}

func main() {
	esURL := getEnv("ES_URL")
	esSnapshotRepo := getEnv("ES_SNAPSHOT_REPO")
	snapShotLimitString := getEnv("ES_SNAPSHOT_LIMIT")
	snapShotLimit, _ := strconv.Atoi(snapShotLimitString)

	t := time.Now()
	esSnapshotId := fmt.Sprintf("%d%02d%02d_%02d%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute())

	s := getOldSnapshots(esURL, esSnapshotRepo, snapShotLimit)
	deleteOldSnapshots(esURL, esSnapshotRepo, s)

	createSnapshot(esURL, esSnapshotRepo, esSnapshotId)
	checkSnapshotStatus(esURL, esSnapshotRepo, esSnapshotId)
}

func getEnv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("required environment variable %s is not set", key)
	}
	return v
}

func getOldSnapshots(url string, repo string, snapshotLimit int) []Snapshot {
	fmt.Print("Getting old snapshots...")
	client := http.Client{Timeout: 1 * time.Minute}
	req, _ := http.NewRequest("GET", url+"/_snapshot/"+repo+"/_all", nil)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Getting old snapshots failed")
	}
	defer resp.Body.Close()

	esr := ESResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&esr); err != nil {
		log.Fatal("error decoding response", err)
	}

	if len(esr.Snapshots) <= snapshotLimit {
		fmt.Println("nothing to delete")
		return nil
	}
	s := esr.Snapshots[:len(esr.Snapshots)-snapshotLimit]
	fmt.Println("done")
	return s
}

func deleteOldSnapshots(url string, repo string, ids []Snapshot) {
	if len(ids) < 1 {
		return
	}
	fmt.Println("Deleting old snapshots...")
	client := http.Client{Timeout: 10 * time.Minute}
	for _, v := range ids {
		fmt.Printf("Deleting ID: %s ...", v.Snapshot)
		req, _ := http.NewRequest("DELETE", url+"/_snapshot/"+repo+"/"+v.Snapshot, nil)
		resp, err := client.Do(req)
		defer resp.Body.Close()

		if err != nil {
			fmt.Println("Snapshot DELETE request failed")
			continue
		}

		s := ESAckStatus{}
		if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
			fmt.Println("error decoding response", err)
			continue
		}

		if s.Acknowledged != true {
			fmt.Println("Snapshot deletion failed")
			continue
		}
		fmt.Println("done")

	}
}

func createSnapshot(url string, repo string, snapId string) {
	fmt.Printf("Creating snapshot %v, repo: %v ...", snapId, repo)
	client := http.Client{Timeout: 30 * time.Minute}
	req, _ := http.NewRequest("PUT", url+"/_snapshot/"+repo+"/"+snapId, nil)
	q := req.URL.Query()
	q.Add("wait_for_completion", "true")
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)

	if err != nil {
		log.Fatal("Snapshot create request failed")
	}

	defer resp.Body.Close()
	fmt.Println("done")

	if err != nil {
		log.Fatal("Snapshot creation failed", err)
	}
}

func checkSnapshotStatus(url string, repo string, snapId string) {
	fmt.Printf("Checking snapshot %s state: ", snapId)
	client := http.Client{Timeout: 1 * time.Minute}
	req, _ := http.NewRequest("GET", url+"/_snapshot/"+repo+"/"+snapId, nil)
	resp, _ := client.Do(req)
	defer resp.Body.Close()

	esr := ESResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&esr); err != nil {
		log.Fatal("error decoding response", err)
	}
    s := esr.Snapshots[0].State
	if s == "SUCCESS" {
		fmt.Println(s)
		os.Exit(0)
	} else {
		fmt.Println(s)
		os.Exit(1)
	}
}
