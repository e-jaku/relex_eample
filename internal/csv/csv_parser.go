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

type (
	CsvParser struct {
		concurrency int
	}
)

func NewCsvParser(concurrency int) *CsvParser {
	if concurrency < 1 {
		concurrency = 10 // default to 10
	}
	return &CsvParser{
		concurrency: concurrency,
	}
}

func (p *CsvParser) ParseFile(ctx context.Context, file io.Reader) (*domain.Node, error) {
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

func extractHierarchyLevels(row []string, colIndexes map[string]int) ([]string, error) {
	levels := []string{}
	currentParent := ""

	for i := 1; i <= len(colIndexes)-1; i++ {
		colName := fmt.Sprintf("%s%d", LV_PREFIX, i)
		index, ok := colIndexes[colName]
		if !ok {
			return nil, xerrors.Errorf("invalid index found: %s", colName)
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
