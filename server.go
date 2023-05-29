package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"github.com/xeipuuv/gojsonschema"
	"google.golang.org/grpc"

	pb "path/to/your/proto/package" // Update with your proto package
	"github.com/lib/pq"
)

// MessageServer is the implementation of the gRPC MessageService server.
type MessageServer struct {
	db     *sql.DB
	cache  *redis.Client
	writer *kafka.Writer
}

// DefineMessageFormat defines the message format with the given name and DSL code.
func (s *MessageServer) DefineMessageFormat(ctx context.Context, req *pb.DefineMessageFormatRequest) (*pb.DefineMessageFormatResponse, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
		if err != nil {
			log.Printf("failed to commit transaction: %v", err)
		}
	}()

	// Insert or update the message format in PostgreSQL
	_, err = tx.Exec(`
		INSERT INTO message_formats (name, dsl_code)
		VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET dsl_code = $2
	`, req.Name, req.Schema.DslCode)
	if err != nil {
		return nil, fmt.Errorf("failed to define message format: %v", err)
	}

	// Invalidate the DSL code in the cache
	err = s.cache.Del(ctx, req.Name).Err()
	if err != nil {
		log.Printf("failed to invalidate DSL code in cache: %v", err)
	}

	return &pb.DefineMessageFormatResponse{
		Success: true,
	}, nil
}

// ProcessMessage processes the received message by validating it against the defined schema
// and publishes it to a Kafka topic if it is valid.
func (s *MessageServer) ProcessMessage(ctx context.Context, req *pb.MessageRequest) (*pb.ProcessMessageResponse, error) {
	// Retrieve the DSL code from cache or database
	dslCode, err := s.getDSLCodeFromCacheOrDB(req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve message format: %v", err)
	}

	// Validate the payload against the schema
	err = ValidatePayload(dslCode, req.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to validate payload: %v", err)
	}

	// Process the valid message here...
	fmt.Println("Valid message received:", req.Name, req.Payload)

	// Publish the validated message to a Kafka topic
	err = s.publishToKafka(req.Name, req.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message to Kafka: %v", err)
	}

	return &pb.ProcessMessageResponse{
		Success: true,
	}, nil
}

// ValidatePayload validates the given payload against the DSL schema.
func ValidatePayload(dslCode, payload string) error {
	// Load the DSL code and message payload as JSON
	schemaLoader := gojsonschema.NewStringLoader(dslCode)
	payloadLoader := gojsonschema.NewStringLoader(payload)

	// Validate the payload against the schema
	result, err := gojsonschema.Validate(schemaLoader, payloadLoader)
	if err != nil {
		return fmt.Errorf("failed to validate payload: %v", err)
	}

	if !result.Valid() {
		// Collect validation errors
		var errors []string
		for _, err := range result.Errors() {
			errors = append(errors, err.String())
		}
		return fmt.Errorf("payload validation failed: %s", strings.Join(errors, "; "))
	}

	return nil
}

// getDSLCodeFromCacheOrDB fetches the DSL code from the cache or the database.
func (s *MessageServer) getDSLCodeFromCacheOrDB(name string) (string, error) {
	// Try to fetch the DSL code from the cache
	dslCode, err := s.cache.Get(ctx, name).Result()
	if err == nil {
		// Cache hit, return the DSL code
		return dslCode, nil
	} else if err != redis.Nil {
		// Error occurred while accessing the cache
		return "", fmt.Errorf("failed to fetch DSL code from cache: %v", err)
	}

	// Cache miss, fetch the DSL code from the database
	err = s.db.QueryRow("SELECT dsl_code FROM message_formats WHERE name = $1", name).Scan(&dslCode)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("message format not found")
	} else if err != nil {
		return "", fmt.Errorf("failed to fetch DSL code from database: %v", err)
	}

	// Store the DSL code in the cache
	err = s.cache.Set(ctx, name, dslCode, 0).Err()
	if err != nil {
		log.Printf("failed to store DSL code in cache: %v", err)
	}

	return dslCode, nil
}

// publishToKafka publishes the validated message to a Kafka topic.
func (s *MessageServer) publishToKafka(name, payload string) error {
	message := kafka.Message{
		Key:   []byte(name),
		Value: []byte(payload),
	}

	err := s.writer.WriteMessages(context.Background(), message)
	if err != nil {
		return fmt.Errorf("failed to publish message to Kafka: %v", err)
	}

	return nil
}

func main() {
	// Retrieve the database connection details from environment variables
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	// Construct the database connection string
	dbConnectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	// Connect to the database
	db, err := sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Retrieve the Kafka broker addresses from environment variables
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")

	// Create a Kafka producer
	producer, err := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  strings.Split(kafkaBrokers, ","),
		Topic:    "your-kafka-topic",
		Balancer: &kafka.Hash{},
	})
	if err != nil {
		log.Fatalf("failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Retrieve the Redis connection details from environment variables
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisPassword := os.Getenv("REDIS_PASSWORD")

	// Create a Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: redisPassword,
		DB:       0,
	})

	// Check if the Redis connection is successful
	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}

	// Create a gRPC server
	server := grpc.NewServer()

	// Create a MessageServer instance
	messageServer := &MessageServer{
		db:     db,
		cache:  redisClient,
		writer: producer,
	}

	// Register the MessageService server
	pb.RegisterMessageServiceServer(server, messageServer)

	// Start the gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
	defer lis.Close()

	log.Println("Server started")

	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	log.Println("Server stopped")
}
