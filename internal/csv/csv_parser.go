package csv

import (
	"context"
	"encoding/csv"
	"io"

	"github.com/formulatehq/data-engineer/internal/domain"
	"github.com/formulatehq/data-engineer/internal/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

const (
	LV1     string = "level_1"
	LV2     string = "level_2"
	LV3     string = "level_3"
	ITEM_ID string = "item_id"
)

type (
	columnIndexes struct {
		LV1Index    int
		LV2Index    int
		LV3Index    int
		ItemIDIndex int
	}
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

	if err := validateColIndexes(colIndexes); err != nil {
		return nil, xerrors.Errorf("failed validating column indexes: %w", err)
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

func parseHeader(csvReader *csv.Reader) (columnIndexes, error) {
	header, err := csvReader.Read()
	if err != nil {
		return columnIndexes{}, xerrors.Errorf("failed to read CSV header: %w", err)
	}

	colIndexes, err := extractColIndexes(header)
	if err != nil {
		return columnIndexes{}, xerrors.Errorf("failed to extract column indexes: %w", err)
	}

	return colIndexes, nil
}

func extractColIndexes(columns []string) (columnIndexes, error) {
	colIndexes := columnIndexes{
		LV1Index:    -1,
		LV2Index:    -1,
		LV3Index:    -1,
		ItemIDIndex: -1,
	}

	indexMap := map[string]*int{
		LV1:     &colIndexes.LV1Index,
		LV2:     &colIndexes.LV2Index,
		LV3:     &colIndexes.LV3Index,
		ITEM_ID: &colIndexes.ItemIDIndex,
	}

	for index, col := range columns {
		if idxPtr, exists := indexMap[col]; exists {
			if *idxPtr != -1 {
				return colIndexes, xerrors.Errorf("failed to extract column index for %s: %w", col, errors.ErrReoccurringColumn)
			}
			*idxPtr = index
		} else {
			return colIndexes, xerrors.Errorf("unknown column type in header %s: %w", col, errors.ErrUnknownColumn)
		}
	}

	return colIndexes, nil
}

func validateColIndexes(colIndexes columnIndexes) error {
	if colIndexes.LV1Index == -1 {
		return xerrors.Errorf("missing col %s: %w", LV1, errors.ErrMissingRequiredColumn)
	}

	if colIndexes.ItemIDIndex == -1 {
		return xerrors.Errorf("missing col %s: %w", ITEM_ID, errors.ErrMissingRequiredColumn)
	}

	if colIndexes.LV3Index != -1 {
		// LV3 defined , makes LV2 required
		if colIndexes.LV2Index == -1 {
			return xerrors.Errorf("missing col %s required due to presence of %s: %w", LV2, LV3, errors.ErrMissingRequiredColumn)
		}
	}
	return nil
}

func parseRow(ctx context.Context, rowChan <-chan []string, nodes *domain.Node, colIndexes columnIndexes) error {
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

			if colIndexes.ItemIDIndex >= len(row) {
				return xerrors.Errorf("failed to parse invalid row %v missing %s: %w", row, ITEM_ID, errors.ErrMissingRequiredValue)
			}

			nodes.AddNode(levels, row[colIndexes.ItemIDIndex])
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func extractHierarchyLevels(row []string, colIndexes columnIndexes) ([]string, error) {
	var levels []string

	if colIndexes.LV1Index >= len(row) {
		return nil, xerrors.Errorf("failed to parse invalid row %v, missing %s: %w", row, LV1, errors.ErrMissingRequiredValue)
	}

	lvCurrent := row[colIndexes.LV1Index]
	if lvCurrent != "" {
		levels = append(levels, lvCurrent) // add first lv
	}

	lvNext := ""
	if colIndexes.LV2Index != -1 {
		if colIndexes.LV2Index >= len(row) {
			return nil, xerrors.Errorf("failed to parse invalid row %v, missing %s: %w", row, LV2, errors.ErrMissingRequiredValue)
		}

		lvNext = row[colIndexes.LV2Index]
		if lvCurrent == "" && lvNext != "" {
			// invalid n empty n+1 non-empty case
			return nil, xerrors.Errorf("failed to parse invalid row %v: %w", row, errors.ErrMissingParentElement)
		}
		if lvNext != "" {
			levels = append(levels, lvNext) // add optional lv2
		}
		lvCurrent = lvNext
	}

	if colIndexes.LV3Index != -1 {
		if colIndexes.LV3Index >= len(row) {
			return nil, xerrors.Errorf("failed to parse invalid row %v, missing %s: %w", row, LV3, errors.ErrMissingRequiredValue)
		}
		lvNext = row[colIndexes.LV3Index]
		if lvCurrent == "" && lvNext != "" {
			// invalid n empty n+1 non-empty case
			return nil, xerrors.Errorf("failed to parse invalid row %v: %w", row, errors.ErrMissingParentElement)
		}
		if lvNext != "" {
			levels = append(levels, lvNext) // add optional lv3
		}
	}

	return levels, nil
}
