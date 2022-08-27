package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

const table = "users2"

var client *dynamodb.Client

func init() {

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal("failed to get dynamodb client", err)
	}
	client = dynamodb.NewFromConfig(cfg)

}

func main() {

	//basicBatchImport()
	basicBatchImport2(10000)
	//for i := 1; i <= 100; i++ {
	//parallelBatchImport(10000)
	//}

	//scan()
	//paginatedScan(3000)
	//parallelScan(10000, 100)

}

type User struct {
	Email string `dynamodbav:"email"`
	Age   int    `dynamodbav:"age,omitempty"`
	City  string `dynamodbav:"city"`
}

// basic scan example
func scan() {

	startTime := time.Now()

	op, err := client.Scan(context.Background(), &dynamodb.ScanInput{
		TableName:              aws.String(table),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})

	if err != nil {
		log.Fatal("scan failed", err)
	}

	for _, i := range op.Items {
		var u User
		err := attributevalue.UnmarshalMap(i, &u)
		if err != nil {
			log.Fatal("unmarshal failed", err)
		}
	}

	if op.LastEvaluatedKey != nil {
		log.Println("all items have not been scanned")
	}
	log.Println("scanned", op.ScannedCount, "items in", time.Since(startTime).Seconds(), "seconds")
	log.Println("consumed capacity", *op.ConsumedCapacity.CapacityUnits)
}

// scan with limit
func paginatedScan(pageSize int) {

	log.Println("paginated scan in progress.....")

	startTime := time.Now()
	var total int

	lastEvaluatedKey := make(map[string]types.AttributeValue)

	scip := &dynamodb.ScanInput{
		TableName: aws.String(table),
		Limit:     aws.Int32(int32(pageSize)),
	}

	for {
		if len(lastEvaluatedKey) != 0 {
			scip.ExclusiveStartKey = lastEvaluatedKey
		}
		op, err := client.Scan(context.Background(), scip)

		if err != nil {
			log.Fatal("scan failed", err)
		}

		total = total + int(op.Count)

		for _, i := range op.Items {
			var u User
			err := attributevalue.UnmarshalMap(i, &u)
			if err != nil {
				log.Fatal("unmarshal failed", err)
			}
		}

		if len(op.LastEvaluatedKey) == 0 {
			log.Println("no more records to scan")
			log.Println("scanned", total, "items in", time.Since(startTime).Seconds(), "seconds")

			return
		}

		lastEvaluatedKey = op.LastEvaluatedKey
	}

}

// scan with limit and segments
func parallelScan(pageSize, totalWorkers int) {
	log.Println("parallel scan with page size", pageSize, "and", totalWorkers, "goroutines")
	startTime := time.Now()

	var total int

	var wg sync.WaitGroup
	wg.Add(totalWorkers)

	for i := 0; i < totalWorkers; i++ {
		// start a goroutine for each segment

		go func(segId int) {
			var segTotal int

			defer wg.Done()

			lastEvaluatedKey := make(map[string]types.AttributeValue)

			scip := &dynamodb.ScanInput{
				TableName:     aws.String(table),
				Limit:         aws.Int32(int32(pageSize)),
				Segment:       aws.Int32(int32(segId)),
				TotalSegments: aws.Int32(int32(totalWorkers)),
			}

			for {
				if len(lastEvaluatedKey) != 0 {
					scip.ExclusiveStartKey = lastEvaluatedKey
				}
				op, err := client.Scan(context.Background(), scip)

				if err != nil {
					log.Fatal("scan failed", err)
				}

				segTotal = segTotal + int(op.Count)

				for _, i := range op.Items {

					var u User
					err := attributevalue.UnmarshalMap(i, &u)
					if err != nil {
						log.Fatal("unmarshal failed", err)
					}
				}

				if len(op.LastEvaluatedKey) == 0 {
					log.Println("[ segment", segId, "] finished")
					total = total + segTotal
					log.Println("total records processsed by segment", segId, "=", segTotal)
					return
				}

				lastEvaluatedKey = op.LastEvaluatedKey
			}
		}(i)
	}

	log.Println("waiting...")
	wg.Wait()

	log.Println("done...")
	log.Println("scanned", total, "items in", time.Since(startTime).Seconds(), "seconds")
}

func parallelBatchImport(numRecords int) {

	startTime := time.Now()

	cities := []string{"NJ", "NY", "ohio"}
	batchSize := 25

	var wg sync.WaitGroup

	processed := numRecords

	for num := 1; num <= numRecords; num = num + batchSize {
		start := num
		end := num + 24

		wg.Add(1)

		go func(s, e int) {
			defer wg.Done()

			batch := make(map[string][]types.WriteRequest)
			var requests []types.WriteRequest

			for i := s; i <= e; i++ {
				user := User{Email: uuid.NewString() + "@foo.com", Age: rand.Intn(49) + 1, City: cities[rand.Intn(len(cities))]}

				item, err := attributevalue.MarshalMap(user)
				if err != nil {
					log.Fatal("marshal map failed", err)
				}
				requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
			}

			batch[table] = requests

			op, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
				RequestItems: batch,
			})

			if err != nil {
				log.Fatal("batch write error", err)
			}

			if len(op.UnprocessedItems) != 0 {
				processed = processed - len(op.UnprocessedItems)
			}

		}(start, end)
	}

	log.Println("waiting for all batches to finish....")
	wg.Wait()

	log.Println("all batches finished. inserted", processed, "records in", time.Since(startTime).Seconds(), "seconds")

	if processed != numRecords {
		log.Println("there were", (numRecords - processed), "unprocessed records")
	}
}

func basicBatchImport() {

	startTime := time.Now()

	cities := []string{"NJ", "NY", "ohio"}

	batch := make(map[string][]types.WriteRequest)
	var requests []types.WriteRequest

	for i := 1; i <= 25; i++ {
		user := User{Email: uuid.NewString() + "@foo.com", Age: rand.Intn(49) + 1, City: cities[rand.Intn(len(cities))]}
		item, _ := attributevalue.MarshalMap(user)
		requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}

	batch[table] = requests

	op, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: batch,
	})
	if err != nil {
		log.Fatal("batch write error", err)
	} else {
		log.Println("batch insert done")
	}

	if len(op.UnprocessedItems) != 0 {
		log.Println("there were", len(op.UnprocessedItems), "unprocessed records")
	}

	log.Println("inserted", (25 - len(op.UnprocessedItems)), "records in", time.Since(startTime).Seconds(), "seconds")

}

func basicBatchImport2(total int) {

	startTime := time.Now()

	cities := []string{"NJ", "NY", "ohio"}

	batchSize := 25
	//batchNum := 1
	processed := total

	for num := 1; num <= total; num = num + batchSize {

		batch := make(map[string][]types.WriteRequest)
		var requests []types.WriteRequest

		start := num
		end := num + 24

		for i := start; i <= end; i++ {
			user := User{Email: uuid.NewString() + "@foo.com", Age: rand.Intn(49) + 1, City: cities[rand.Intn(len(cities))]}
			item, _ := attributevalue.MarshalMap(user)
			requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
		}

		batch[table] = requests

		op, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
			RequestItems: batch,
		})

		if err != nil {
			log.Fatal("batch write error", err)
		}

		if len(op.UnprocessedItems) != 0 {
			processed = processed - len(op.UnprocessedItems)
		}
	}

	log.Println("all batches finished. inserted", processed, "records in", time.Since(startTime).Seconds(), "seconds")

	if processed != total {
		log.Println("there were", (total - processed), "unprocessed records")
	}
}
