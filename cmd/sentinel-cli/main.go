package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/matteso1/sentinel/proto"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "produce":
		produceCmd()
	case "consume":
		consumeCmd()
	case "topics":
		topicsCmd()
	case "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Sentinel CLI - Distributed Log Streaming

Usage:
  sentinel-cli <command> [options]

Commands:
  produce     Send messages to a topic
  consume     Read messages from a topic
  topics      List or manage topics
  help        Show this help

Examples:
  sentinel-cli produce -topic events -message "Hello, World!"
  sentinel-cli consume -topic events -from-beginning
  sentinel-cli topics -list`)
}

func produceCmd() {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	broker := fs.String("broker", "localhost:9092", "Broker address")
	topic := fs.String("topic", "", "Topic name (required)")
	message := fs.String("message", "", "Message to send")
	key := fs.String("key", "", "Message key (optional)")
	partition := fs.Int("partition", -1, "Partition (-1 for auto)")
	count := fs.Int("count", 1, "Number of messages to send")

	fs.Parse(os.Args[2:])

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "Error: -topic is required")
		os.Exit(1)
	}

	conn, err := grpc.Dial(*broker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewSentinelClient(conn)

	records := make([]*pb.Record, *count)
	for i := 0; i < *count; i++ {
		msg := *message
		if *count > 1 {
			msg = fmt.Sprintf("%s-%d", *message, i)
		}
		records[i] = &pb.Record{
			Key:   []byte(*key),
			Value: []byte(msg),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Produce(ctx, &pb.ProduceRequest{
		Topic:     *topic,
		Partition: int32(*partition),
		Records:   records,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to produce: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✓ Produced %d record(s) to %s (offset: %d)\n", *count, *topic, resp.BaseOffset)
}

func consumeCmd() {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	broker := fs.String("broker", "localhost:9092", "Broker address")
	topic := fs.String("topic", "", "Topic name (required)")
	partition := fs.Int("partition", 0, "Partition to consume from")
	offset := fs.Int64("offset", 0, "Starting offset")
	fromBeginning := fs.Bool("from-beginning", false, "Start from earliest offset")
	maxMessages := fs.Int("max-messages", 100, "Maximum messages to consume")

	fs.Parse(os.Args[2:])

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "Error: -topic is required")
		os.Exit(1)
	}

	startOffset := *offset
	if *fromBeginning {
		startOffset = 0
	}

	conn, err := grpc.Dial(*broker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewSentinelClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx, &pb.ConsumeRequest{
		Topic:     *topic,
		Partition: int32(*partition),
		Offset:    startOffset,
		MaxBytes:  1024 * 1024,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to consume: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Consuming from %s (partition %d, offset %d)...\n", *topic, *partition, startOffset)
	fmt.Println(strings.Repeat("-", 60))

	count := 0
	for {
		if count >= *maxMessages {
			break
		}

		record, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error receiving: %v\n", err)
			break
		}

		keyStr := "(null)"
		if len(record.Key) > 0 {
			keyStr = string(record.Key)
		}

		fmt.Printf("offset=%d | key=%s | value=%s\n", record.Offset, keyStr, string(record.Value))
		count++
	}

	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Consumed %d message(s)\n", count)
}

func topicsCmd() {
	fs := flag.NewFlagSet("topics", flag.ExitOnError)
	broker := fs.String("broker", "localhost:9092", "Broker address")
	list := fs.Bool("list", false, "List all topics")
	describe := fs.String("describe", "", "Describe a topic")

	fs.Parse(os.Args[2:])

	conn, err := grpc.Dial(*broker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewSentinelClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topics := []string{}
	if *describe != "" {
		topics = append(topics, *describe)
	}

	resp, err := client.FetchMetadata(ctx, &pb.MetadataRequest{
		Topics: topics,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to fetch metadata: %v\n", err)
		os.Exit(1)
	}

	if *list || *describe != "" {
		fmt.Println("Topics:")
		for _, t := range resp.Topics {
			fmt.Printf("  %s (%d partitions)\n", t.Name, len(t.Partitions))
			if *describe != "" {
				for _, p := range t.Partitions {
					fmt.Printf("    partition=%d leader=%d replicas=%v isr=%v\n",
						p.PartitionId, p.Leader, p.Replicas, p.Isr)
				}
			}
		}
	}

	if !*list && *describe == "" {
		fmt.Println("Brokers:")
		for _, b := range resp.Brokers {
			fmt.Printf("  %d: %s:%d\n", b.Id, b.Host, b.Port)
		}
	}
}
