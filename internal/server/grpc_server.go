package server

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/matteso1/sentinel/proto"
	"github.com/matteso1/sentinel/internal/storage"
)

// Server implements the Sentinel gRPC service.
type Server struct {
	pb.UnimplementedSentinelServer

	// Storage engine
	store *storage.LSM
	
	// Topic/partition management
	topics map[string]*Topic
	mu     sync.RWMutex
	
	// gRPC server
	grpcServer *net.Listener
	grpc       *grpc.Server
	
	// Config
	config ServerConfig
}

// Topic represents a message topic with partitions.
type Topic struct {
	name       string
	partitions []*Partition
}

// Partition is an append-only log within a topic.
type Partition struct {
	id         int32
	nextOffset int64
	mu         sync.Mutex
}

// ServerConfig configures the server.
type ServerConfig struct {
	Port        int
	DataDir     string
	LSMConfig   storage.LSMConfig
}

// DefaultServerConfig returns sensible defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Port:      9092,
		DataDir:   "./data",
		LSMConfig: storage.DefaultLSMConfig(),
	}
}

// NewServer creates a new Sentinel server.
func NewServer(config ServerConfig) (*Server, error) {
	// Open storage
	store, err := storage.Open(config.DataDir, config.LSMConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage: %w", err)
	}
	
	return &Server{
		store:  store,
		topics: make(map[string]*Topic),
		config: config,
	}, nil
}

// Start starts the gRPC server.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	
	s.grpc = grpc.NewServer()
	pb.RegisterSentinelServer(s.grpc, s)
	
	fmt.Printf("Sentinel server listening on port %d\n", s.config.Port)
	return s.grpc.Serve(lis)
}

// Stop gracefully stops the server.
func (s *Server) Stop() {
	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
	if s.store != nil {
		s.store.Close()
	}
}

// Produce handles produce requests.
func (s *Server) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	
	s.mu.Lock()
	topic, exists := s.topics[req.Topic]
	if !exists {
		// Auto-create topic with 1 partition
		topic = &Topic{
			name:       req.Topic,
			partitions: []*Partition{{id: 0, nextOffset: 0}},
		}
		s.topics[req.Topic] = topic
	}
	s.mu.Unlock()
	
	// Validate partition
	if int(req.Partition) >= len(topic.partitions) {
		return nil, status.Errorf(codes.InvalidArgument, "partition %d does not exist", req.Partition)
	}
	
	partition := topic.partitions[req.Partition]
	partition.mu.Lock()
	baseOffset := partition.nextOffset
	
	// Write each record to storage
	for i, record := range req.Records {
		offset := baseOffset + int64(i)
		key := s.makeKey(req.Topic, req.Partition, offset)
		
		if err := s.store.Put(key, record.Value); err != nil {
			partition.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "failed to write: %v", err)
		}
		
		record.Offset = offset
	}
	
	partition.nextOffset = baseOffset + int64(len(req.Records))
	partition.mu.Unlock()
	
	return &pb.ProduceResponse{
		BaseOffset: baseOffset,
		Timestamp:  0, // TODO: use actual timestamp
	}, nil
}

// Consume streams records from a partition.
func (s *Server) Consume(req *pb.ConsumeRequest, stream pb.Sentinel_ConsumeServer) error {
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}
	
	s.mu.RLock()
	topic, exists := s.topics[req.Topic]
	s.mu.RUnlock()
	
	if !exists {
		return status.Errorf(codes.NotFound, "topic %s not found", req.Topic)
	}
	
	if int(req.Partition) >= len(topic.partitions) {
		return status.Errorf(codes.InvalidArgument, "partition %d does not exist", req.Partition)
	}
	
	offset := req.Offset
	if offset < 0 {
		offset = 0 // Start from beginning
	}
	
	// Stream records
	partition := topic.partitions[req.Partition]
	for {
		partition.mu.Lock()
		maxOffset := partition.nextOffset
		partition.mu.Unlock()
		
		if offset >= maxOffset {
			// No more data, wait or return
			// For now, just return
			return nil
		}
		
		key := s.makeKey(req.Topic, req.Partition, offset)
		value, err := s.store.Get(key)
		if err != nil {
			offset++
			continue
		}
		
		record := &pb.Record{
			Offset: offset,
			Value:  value,
		}
		
		if err := stream.Send(record); err != nil {
			return err
		}
		
		offset++
	}
}

// FetchMetadata returns cluster metadata.
func (s *Server) FetchMetadata(ctx context.Context, req *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	resp := &pb.MetadataResponse{
		Brokers: []*pb.Broker{
			{Id: 0, Host: "localhost", Port: int32(s.config.Port)},
		},
		ControllerId: 0,
	}
	
	// Include requested topics (or all if empty)
	if len(req.Topics) == 0 {
		for name, topic := range s.topics {
			resp.Topics = append(resp.Topics, s.topicToMetadata(name, topic))
		}
	} else {
		for _, name := range req.Topics {
			if topic, exists := s.topics[name]; exists {
				resp.Topics = append(resp.Topics, s.topicToMetadata(name, topic))
			}
		}
	}
	
	return resp, nil
}

func (s *Server) topicToMetadata(name string, topic *Topic) *pb.TopicMetadata {
	tm := &pb.TopicMetadata{Name: name}
	for _, p := range topic.partitions {
		tm.Partitions = append(tm.Partitions, &pb.PartitionMetadata{
			PartitionId: p.id,
			Leader:      0,
			Replicas:    []int32{0},
			Isr:         []int32{0},
		})
	}
	return tm
}

// makeKey creates a storage key for topic/partition/offset.
func (s *Server) makeKey(topic string, partition int32, offset int64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%020d", topic, partition, offset))
}
