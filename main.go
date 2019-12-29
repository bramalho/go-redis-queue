package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/adjust/redismq"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
	}

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisPass := os.Getenv("REDIS_PASS")
	redisDB, _ := strconv.ParseInt(os.Getenv("REDIS_DB"), 10, 64)
	redisQueue := os.Getenv("REDIS_QUEUE")

	queue := redismq.CreateQueue(redisHost, redisPort, redisPass, redisDB, redisQueue)
	for i := 0; i < 0; i++ {
		queue.Put("Payload " + strconv.Itoa(i))
	}

	consumer, err := queue.AddConsumer("my_consumer")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		p, err := consumer.Get()
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Println("Package " + strconv.Itoa(i))
		fmt.Println(p.CreatedAt)
		fmt.Println(p.Payload)

		err = p.Ack()
		if err != nil {
			fmt.Println(err)
		}
	}
}
