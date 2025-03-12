package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BucketInfo struct {
	Name    string
	Size    int64
	Objects int
}

type SlackPayload struct {
	Channel   string `json:"channel"`
	Text      string `json:"text"`
	IconEmoji string `json:"icon_emoji"`
}

func reportStorageSize(results map[string]*BucketInfo) {
	var totalSizeB int64

	for _, bucketInfo := range results {
		totalSizeB += bucketInfo.Size
	}
	totalSizeGB := float64(totalSizeB) / math.Pow(1024, 3)

	log.Printf("Total S3 size: %.2f GiB", totalSizeGB)

	slackPayload := SlackPayload{
		Channel:   os.Getenv("SLACK_CHANNEL"),
		Text:      fmt.Sprintf("Daily S3 size report: %.2f GiB", totalSizeGB),
		IconEmoji: ":bucket:",
	}

	payload, err := json.Marshal(slackPayload)
	if err != nil {
		log.Fatalf("Failed encoding payload: %v", err)
	}

	req, err := http.NewRequest("POST", os.Getenv("SLACK_WEBHOOK_URL"), bytes.NewBuffer(payload))
	if err != nil {
		log.Fatalf("Failed building POST request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed POSTing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Fatalf("Server returned status code %d: %v", resp.StatusCode, err)
	}
}

func main() {
	results := make(map[string]*BucketInfo)

	log.Print("Starting daily S3 size report")

	ctx := context.Background()
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}

	s3Client := s3.NewFromConfig(sdkConfig)
	output, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	var mu sync.RWMutex

	for _, bucket := range output.Buckets {
		wg.Add(1)

		go func(bucketName string, s3Client *s3.Client, ctx context.Context, results map[string]*BucketInfo) {
			defer wg.Done()

			var bucketSize int64
			var objectsCount int

			input := &s3.ListObjectsV2Input{
				Bucket: aws.String(bucketName),
			}

			objectPaginator := s3.NewListObjectsV2Paginator(s3Client, input)
			for objectPaginator.HasMorePages() {
				page, err := objectPaginator.NextPage(ctx)
				if err != nil {
					log.Printf("Bucket %s skipped", *page.Name)
					continue
				}

				for _, object := range page.Contents {
					bucketSize += *object.Size
					objectsCount++
				}
			}

			mu.Lock()
			results[bucketName] = &BucketInfo{
				Name:    bucketName,
				Size:    bucketSize,
				Objects: objectsCount,
			}
			mu.Unlock()
		}(*bucket.Name, s3Client, ctx, results)
	}

	wg.Wait()

	reportStorageSize(results)
}
