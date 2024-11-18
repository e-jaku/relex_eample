package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/formulatehq/data-engineer/internal/domain"
	"github.com/formulatehq/data-engineer/internal/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

const (
	ITEM_ID   string = "item_id"
	LV_PREFIX string = "level_"
)

type CSVParser struct {
	concurrency int
}

func NewCSVParser(concurrency int) *CSVParser {
	if concurrency < 1 {
		concurrency = 10 // default to 10
	}
	return &CSVParser{
		concurrency: concurrency,
	}
}

// ParseFile reads an parses a CSV file. Each row read is pushed to a buffered channel and
// processed concurrently by a worker goroutine.
// If the ctx is canceled or an error occurs during reading the reading process is aborted and the error returned.
// Returns a domain.Node object containing the built hierarchy.
func (p *CSVParser) ParseFile(ctx context.Context, file io.Reader) (*domain.Node, error) {
	nodes := domain.NewNode()
	csvReader := csv.NewReader(file)

	colIndexes, err := parseHeader(csvReader)
	if err != nil {
		return nil, xerrors.Errorf("failed to extract column indexes: %w", err)
	}

	errG, ctx := errgroup.WithContext(ctx)
	rowChan := make(chan []string, 100)

	errG.Go(func() error {
		defer close(rowChan)
		for {
			row, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return xerrors.Errorf("failed to read CSV row: %w", err)
			}
			select {
			case rowChan <- row:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	for i := 0; i < p.concurrency; i++ {
		errG.Go(func() error {
			return parseRow(ctx, rowChan, nodes, colIndexes)
		})
	}

	if err := errG.Wait(); err != nil {
		return nil, xerrors.Errorf("failed processing rows: %w", err)
	}

	return nodes, nil
}

// parseHeader reads the first row of a CSV and deduces the column indexes of each column.
// The presence of the required columns, reoccurrence and column hierarchy is validated.
func parseHeader(csvReader *csv.Reader) (map[string]int, error) {
	header, err := csvReader.Read()
	if err != nil {
		return nil, xerrors.Errorf("failed to read CSV header: %w", err)
	}

	colIndexes, err := extractColIndexes(header)
	if err != nil {
		return nil, xerrors.Errorf("failed to extract column indexes: %w", err)
	}

	return colIndexes, nil
}

// extractColIndexes extracts the columns indexes from the read row.
// The column indexes are returned as a map[string]int with the key being the level name f.e. "level_1" and
// the value the index of the column this level appears.
// Presence of level "n" when "n + 1" appears is validated.
// The method returns an error if a column is present more then once f.e. ["level_1", "level_2", "level_1"].
// The method also returns an error if "level_1" and "item_id" are not present since they are defined as required in this scenario.
func extractColIndexes(columns []string) (map[string]int, error) {
	colIndexes := make(map[string]int)
	seenLevels := make(map[int]bool)

	for index, col := range columns {
		if col == ITEM_ID {
			if _, exists := colIndexes[ITEM_ID]; exists {
				return nil, xerrors.Errorf("duplicate column %s found: %w", ITEM_ID, errors.ErrReoccurringColumn)
			}
			colIndexes[ITEM_ID] = index
			continue
		}

		if strings.HasPrefix(col, LV_PREFIX) {
			levelStr := strings.TrimPrefix(col, LV_PREFIX)
			levelNum, err := strconv.Atoi(levelStr)
			if err != nil {
				return nil, xerrors.Errorf("invalid level column %s: %w", col, errors.ErrUnknownColumn)
			}
			if seenLevels[levelNum] {
				return nil, xerrors.Errorf("duplicate level column %s found: %w", col, errors.ErrReoccurringColumn)
			}
			colIndexes[col] = index
			seenLevels[levelNum] = true
		} else {
			return nil, xerrors.Errorf("unknown column type in header %s: %w", col, errors.ErrUnknownColumn)
		}
	}

	if _, exists := colIndexes[ITEM_ID]; !exists {
		return nil, xerrors.Errorf("missing required column %s: %w", ITEM_ID, errors.ErrMissingRequiredColumn)
	}

	if _, exists := colIndexes[fmt.Sprintf("%s1", LV_PREFIX)]; !exists {
		return nil, xerrors.Errorf("missing required column %s: %w", fmt.Sprintf("%s1", LV_PREFIX), errors.ErrMissingRequiredColumn)
	}

	for level := range seenLevels {
		if level > 1 && !seenLevels[level-1] {
			return nil, xerrors.Errorf("level n+1 requires level n to be present %v: %w", columns, errors.ErrMissingParentElement)
		}
	}

	return colIndexes, nil
}

// parseRow reads from the buffered channel the pushed row and adds it to the node hierarchy.
// If the ctx is canceled, the channel is closed or the parsing of the row fails the execution is aborted.
func parseRow(ctx context.Context, rowChan <-chan []string, nodes *domain.Node, colIndexes map[string]int) error {
	for {
		select {
		case row, ok := <-rowChan:
			if !ok {
				return nil
			}

			levels, err := extractHierarchyLevels(row, colIndexes)
			if err != nil {
				return xerrors.Errorf("failed to get hierarchy levels: %w", err)
			}

			itemIndex, ok := colIndexes[ITEM_ID]
			if !ok || itemIndex >= len(row) {
				return xerrors.Errorf("failed to parse row %v missing %s: %w", row, ITEM_ID, errors.ErrMissingRequiredValue)
			}

			nodes.AddNode(levels, row[itemIndex])
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// extractHierarchyLevels builds the necessary levels contained in a row for adding to the node hierarchy.
// If a row does contain less elements than specified in the header or if a parent element is empty while the child
// element is present this method will return an error.
// This method returns the constructed level hierarchy in increasing depth order f.e.
// {"level_1", "level_2", "level_3"}
func extractHierarchyLevels(row []string, colIndexes map[string]int) ([]string, error) {
	levels := []string{}
	currentParent := ""

	for i := 1; i <= len(colIndexes)-1; i++ {
		colName := fmt.Sprintf("%s%d", LV_PREFIX, i)
		index, ok := colIndexes[colName]
		if !ok {
			return nil, xerrors.Errorf("missing index for column: %s", colName)
		}

		if index >= len(row) {
			return nil, xerrors.Errorf("failed to parse invalid row %v, missing %s: %w", row, colName, errors.ErrMissingRequiredValue)
		}

		currentLevel := row[index]
		if i != 1 && currentParent == "" && currentLevel != "" {
			return nil, xerrors.Errorf("failed to parse invalid row %v: %w", row, errors.ErrMissingParentElement)
		}
		if currentLevel != "" {
			levels = append(levels, currentLevel)
		}
		currentParent = currentLevel
	}
	return levels, nil
}
