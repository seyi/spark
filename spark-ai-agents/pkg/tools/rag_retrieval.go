package tools

import (
	"context"
	"fmt"

	"github.com/apache/spark/spark-ai-agents/pkg/rag"
)

// RagRetrievalConfig configures the RAG retrieval tool
type RagRetrievalConfig struct {
	// VectorStore is the vector database to use
	VectorStore rag.VectorStore

	// EmbeddingService generates embeddings for queries
	EmbeddingService rag.EmbeddingService

	// DefaultCollection is the default collection to search
	DefaultCollection string

	// DefaultTopK is the default number of results to return
	DefaultTopK int

	// DefaultMinScore is the default minimum similarity score
	DefaultMinScore float64

	// Name is the tool name (defaults to "retrieve_documents")
	Name string

	// Description is the tool description
	Description string
}

// RagRetrievalTool creates a RAG retrieval tool for agents
// This tool allows agents to search through private documents using semantic search
func RagRetrievalTool(config RagRetrievalConfig) *ToolDefinition {
	// Apply defaults
	if config.Name == "" {
		config.Name = "retrieve_documents"
	}
	if config.Description == "" {
		config.Description = "Retrieve relevant documents from the knowledge base using semantic search. Use this to find information from private documents to answer questions accurately."
	}
	if config.DefaultTopK == 0 {
		config.DefaultTopK = 5
	}
	if config.DefaultMinScore == 0 {
		config.DefaultMinScore = 0.7
	}

	return &ToolDefinition{
		Name:        config.Name,
		Description: config.Description,
		Schema: &ToolSchema{
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"query": map[string]interface{}{
						"type":        "string",
						"description": "The search query (will be embedded and used for semantic search)",
					},
					"collection": map[string]interface{}{
						"type":        "string",
						"description": "Optional: Collection/corpus to search (defaults to default collection)",
					},
					"top_k": map[string]interface{}{
						"type":        "integer",
						"description": fmt.Sprintf("Optional: Number of results to return (default: %d)", config.DefaultTopK),
					},
					"min_score": map[string]interface{}{
						"type":        "number",
						"description": fmt.Sprintf("Optional: Minimum similarity score 0.0-1.0 (default: %.2f)", config.DefaultMinScore),
					},
				},
				"required": []string{"query"},
			},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			return handleRagRetrieval(ctx, params, config)
		},
		Enabled: true,
	}
}

// handleRagRetrieval handles the RAG retrieval logic
func handleRagRetrieval(ctx context.Context, params map[string]interface{}, config RagRetrievalConfig) (interface{}, error) {
	// Extract query
	query, ok := params["query"].(string)
	if !ok || query == "" {
		return nil, fmt.Errorf("query parameter is required and must be a non-empty string")
	}

	// Extract optional parameters
	collection := config.DefaultCollection
	if coll, ok := params["collection"].(string); ok && coll != "" {
		collection = coll
	}

	topK := config.DefaultTopK
	if k, ok := params["top_k"].(float64); ok {
		topK = int(k)
	}

	minScore := config.DefaultMinScore
	if score, ok := params["min_score"].(float64); ok {
		minScore = score
	}

	// Validate parameters
	if topK <= 0 || topK > 100 {
		return nil, fmt.Errorf("top_k must be between 1 and 100")
	}
	if minScore < 0.0 || minScore > 1.0 {
		return nil, fmt.Errorf("min_score must be between 0.0 and 1.0")
	}

	// Generate embedding for query
	queryEmbedding, err := config.EmbeddingService.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to generate query embedding: %w", err)
	}

	// Search vector store
	searchQuery := &rag.SearchQuery{
		QueryText:         query,
		Embedding:         queryEmbedding,
		Collection:        collection,
		TopK:              topK,
		MinScore:          minScore,
		IncludeEmbeddings: false, // Don't return embeddings to agent
	}

	results, err := config.VectorStore.Search(ctx, searchQuery)
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}

	// Format results for agent
	formattedResults := make([]map[string]interface{}, len(results))
	for i, result := range results {
		formattedResults[i] = map[string]interface{}{
			"content":  result.Document.Content,
			"score":    result.Score,
			"source":   result.Document.Source,
			"metadata": result.Document.Metadata,
		}
	}

	return map[string]interface{}{
		"query":        query,
		"num_results":  len(results),
		"results":      formattedResults,
		"collection":   collection,
		"top_k":        topK,
		"min_score":    minScore,
		"embedding_model": config.EmbeddingService.ModelName(),
	}, nil
}

// SimpleRagRetrievalTool creates a simple RAG tool with in-memory storage
// Good for testing and demos, not production use
func SimpleRagRetrievalTool() *ToolDefinition {
	vectorStore := rag.NewInMemoryVectorStore()
	embeddingService := rag.NewSimpleEmbeddingService(384) // 384-dim embeddings

	return RagRetrievalTool(RagRetrievalConfig{
		VectorStore:       vectorStore,
		EmbeddingService:  embeddingService,
		DefaultCollection: "default",
		DefaultTopK:       5,
		DefaultMinScore:   0.7,
	})
}

// RagCorpusManager helps manage document ingestion into RAG corpus
type RagCorpusManager struct {
	processor *rag.DocumentProcessor
}

// NewRagCorpusManager creates a RAG corpus manager
func NewRagCorpusManager(
	vectorStore rag.VectorStore,
	embeddingService rag.EmbeddingService,
	chunker rag.TextChunker,
	defaultCollection string,
) *RagCorpusManager {
	processor := rag.NewDocumentProcessor(
		chunker,
		embeddingService,
		vectorStore,
		defaultCollection,
	)

	return &RagCorpusManager{
		processor: processor,
	}
}

// IngestDocument ingests a document into the RAG corpus
func (m *RagCorpusManager) IngestDocument(ctx context.Context, content string, metadata map[string]interface{}) error {
	return m.processor.ProcessDocument(ctx, content, metadata)
}

// IngestDocuments ingests multiple documents
func (m *RagCorpusManager) IngestDocuments(ctx context.Context, contents []string, metadataList []map[string]interface{}) error {
	return m.processor.ProcessDocuments(ctx, contents, metadataList)
}

// RegisterRagTools registers RAG-related tools in the tool registry
func RegisterRagTools(
	registry *ToolRegistry,
	vectorStore rag.VectorStore,
	embeddingService rag.EmbeddingService,
	defaultCollection string,
) error {
	// Create RAG retrieval tool
	ragTool := RagRetrievalTool(RagRetrievalConfig{
		VectorStore:       vectorStore,
		EmbeddingService:  embeddingService,
		DefaultCollection: defaultCollection,
		DefaultTopK:       10,
		DefaultMinScore:   0.7,
	})

	return registry.Register(ragTool)
}
