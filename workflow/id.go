package workflow

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// IDConfig holds configuration for WorkflowID generation
type IDConfig struct {
	// Manual WorkflowID
	WorkflowID string

	// Auto-generation options
	Prefix    string
	Suffix    string
	Template  string
	Variables map[string]string

	// Generation strategy
	Strategy IDStrategy
}

// IDStrategy defines the strategy for WorkflowID generation
type IDStrategy int

const (
	// ManualID uses the provided WorkflowID directly
	ManualID IDStrategy = iota
	// UUIDStrategy generates UUID-based IDs
	UUIDStrategy
	// TimestampStrategy generates timestamp-based IDs
	TimestampStrategy
	// TemplateStrategy uses template with variables
	TemplateStrategy
)

// IDGenerator provides methods to generate WorkflowIDs
type IDGenerator struct {
	config *IDConfig
}

// NewIDGenerator creates a new WorkflowID generator
func NewIDGenerator(config *IDConfig) *IDGenerator {
	if config == nil {
		config = &IDConfig{
			Strategy: UUIDStrategy,
		}
	}
	return &IDGenerator{config: config}
}

// Generate generates a WorkflowID based on the configuration
func (g *IDGenerator) Generate() string {
	switch g.config.Strategy {
	case ManualID:
		return g.config.WorkflowID
	case UUIDStrategy:
		return g.generateUUID()
	case TimestampStrategy:
		return g.generateTimestamp()
	case TemplateStrategy:
		return g.generateFromTemplate()
	default:
		return g.generateUUID()
	}
}

// generateUUID generates a UUID-based WorkflowID
func (g *IDGenerator) generateUUID() string {
	id := uuid.New().String()

	if g.config.Prefix != "" {
		id = fmt.Sprintf("%s-%s", g.config.Prefix, id)
	}

	if g.config.Suffix != "" {
		id = fmt.Sprintf("%s-%s", id, g.config.Suffix)
	}

	return id
}

// generateTimestamp generates a timestamp-based WorkflowID
func (g *IDGenerator) generateTimestamp() string {
	timestamp := time.Now().Format("20060102-150405")

	id := timestamp
	if g.config.Prefix != "" {
		id = fmt.Sprintf("%s-%s", g.config.Prefix, timestamp)
	}

	if g.config.Suffix != "" {
		id = fmt.Sprintf("%s-%s", id, g.config.Suffix)
	}

	return id
}

// generateFromTemplate generates WorkflowID from template
func (g *IDGenerator) generateFromTemplate() string {
	if g.config.Template == "" {
		return g.generateUUID()
	}

	result := g.config.Template

	// Replace predefined variables
	result = strings.ReplaceAll(result, "{uuid}", uuid.New().String())
	result = strings.ReplaceAll(result, "{timestamp}", time.Now().Format("20060102-150405"))
	result = strings.ReplaceAll(result, "{date}", time.Now().Format("2006-01-02"))
	result = strings.ReplaceAll(result, "{time}", time.Now().Format("15:04:05"))
	result = strings.ReplaceAll(result, "{unix}", fmt.Sprintf("%d", time.Now().Unix()))

	// Replace custom variables
	for key, value := range g.config.Variables {
		placeholder := fmt.Sprintf("{%s}", key)
		result = strings.ReplaceAll(result, placeholder, value)
	}

	return result
}

// Utility functions for common WorkflowID patterns

// GenerateSimpleID generates a simple WorkflowID with prefix
func GenerateSimpleID(prefix string) string {
	generator := NewIDGenerator(&IDConfig{
		Prefix:   prefix,
		Strategy: UUIDStrategy,
	})
	return generator.Generate()
}

// GenerateTimestampID generates a timestamp-based WorkflowID
func GenerateTimestampID(prefix string) string {
	generator := NewIDGenerator(&IDConfig{
		Prefix:   prefix,
		Strategy: TimestampStrategy,
	})
	return generator.Generate()
}

// GenerateTemplateID generates WorkflowID from template
func GenerateTemplateID(template string, variables map[string]string) string {
	generator := NewIDGenerator(&IDConfig{
		Template:  template,
		Variables: variables,
		Strategy:  TemplateStrategy,
	})
	return generator.Generate()
}

// Validate validates a WorkflowID
func Validate(workflowID string) error {
	if workflowID == "" {
		return fmt.Errorf("workflow ID cannot be empty")
	}

	if len(workflowID) > 1000 {
		return fmt.Errorf("workflow ID too long: %d characters (max 1000)", len(workflowID))
	}

	// Check for invalid characters (basic validation)
	invalidChars := []string{"\n", "\r", "\t"}
	for _, char := range invalidChars {
		if strings.Contains(workflowID, char) {
			return fmt.Errorf("workflow ID contains invalid character: %q", char)
		}
	}

	return nil
}
