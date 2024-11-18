package csv

import (
	"context"
	"strings"
	"testing"

	"github.com/formulatehq/data-engineer/internal/domain"
	"github.com/formulatehq/data-engineer/internal/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestParseFile(t *testing.T) {
	validFile := `level_1,level_2,level_3,item_id
1,12,103,12507622
1,13,,32622917
`
	reader := strings.NewReader(validFile)
	parser := &CsvParser{concurrency: 10}

	hierarchy, err := parser.ParseFile(context.Background(), reader)
	require.NoError(t, err)
	require.NotNil(t, hierarchy.Children)

	invalidFile := `level_1,level_2,level_3,item_id
,,103,12507622
1,,,32622917
`

	invalidFileReader := strings.NewReader(invalidFile)
	res, err := parser.ParseFile(context.Background(), invalidFileReader)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrMissingParentElement)
	require.Nil(t, res)
}

func TestExtractColIndexes(t *testing.T) {
	tests := []struct {
		name               string
		expectedErrorType  error
		columns            []string
		expectedColIndexes map[string]int
	}{
		{
			name:    "Valid case with missing LV3",
			columns: []string{"level_1", "level_2", ITEM_ID},
			expectedColIndexes: map[string]int{
				"level_1": 0,
				"level_2": 1,
				ITEM_ID:   2,
			},
		},
		{
			name:    "Valid case with all LV",
			columns: []string{"level_1", "level_2", "level_3", ITEM_ID},
			expectedColIndexes: map[string]int{
				"level_1": 0,
				"level_2": 1,
				"level_3": 2,
				ITEM_ID:   3,
			},
		},
		{
			name:    "Valid case with different order",
			columns: []string{"level_2", "level_3", ITEM_ID, "level_1"},
			expectedColIndexes: map[string]int{
				"level_1": 3,
				"level_2": 0,
				"level_3": 1,
				ITEM_ID:   2,
			},
		},
		{
			name:              "Unknown type",
			columns:           []string{"level_1", "level_2", ITEM_ID, "other_unknown_column"},
			expectedErrorType: errors.ErrUnknownColumn,
		},
		{
			name:              "Wrong Reoccurring column",
			columns:           []string{"level_1", "level_2", "level_1"},
			expectedErrorType: errors.ErrReoccurringColumn,
		},
		{
			name:              "missing required LV1",
			columns:           []string{"level_2", ITEM_ID},
			expectedErrorType: errors.ErrMissingRequiredColumn,
		},
		{
			name:              "missing required item_id column",
			columns:           []string{"level_1"},
			expectedErrorType: errors.ErrMissingRequiredColumn,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			colIndexes, err := extractColIndexes(tc.columns)
			if tc.expectedErrorType == nil {
				require.NoError(t, err)
				require.Equal(t, tc.expectedColIndexes, colIndexes)
			} else {
				require.ErrorIs(t, err, tc.expectedErrorType)
			}
		})
	}
}

func TestExtractHierarchyLevels(t *testing.T) {
	tests := []struct {
		name              string
		expectedErrorType error
		row               []string
		colIndexes        map[string]int
		expectedLevels    []string
	}{
		{
			name:           "Valid case",
			row:            []string{"level_1", "level_2", ITEM_ID},
			expectedLevels: []string{"level_1", "level_2"},
			colIndexes: map[string]int{
				"level_1": 0,
				"level_2": 1,
				ITEM_ID:   2,
			},
		},
		{
			name:           "Valid case with mixed order",
			row:            []string{"level_3", "level_1", "level_2", ITEM_ID},
			expectedLevels: []string{"level_1", "level_2", "level_3"},
			colIndexes: map[string]int{
				"level_1": 1,
				"level_2": 2,
				"level_3": 0,
				ITEM_ID:   3,
			},
		},
		{
			name: "Invalid row",
			row:  []string{"some random data"},
			colIndexes: map[string]int{
				"level_1": 1,
				"level_2": 2,
				"level_3": 0,
				ITEM_ID:   3,
			},
			expectedErrorType: errors.ErrMissingRequiredValue,
		},
		{
			name: "Invalid row, missing parent to child element",
			row:  []string{"level_1", "", "level_3", ITEM_ID},
			colIndexes: map[string]int{
				"level_1": 0,
				"level_2": 1,
				"level_3": 2,
				ITEM_ID:   3,
			},
			expectedErrorType: errors.ErrMissingParentElement,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			levels, err := extractHierarchyLevels(tc.row, tc.colIndexes)
			if tc.expectedErrorType == nil {
				require.NoError(t, err)
				require.Equal(t, tc.expectedLevels, levels)
			} else {
				require.ErrorIs(t, err, tc.expectedErrorType)
			}
		})
	}
}

func TestParseRow(t *testing.T) {
	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(context.Background())
	rowChan := make(chan []string, 1)
	nodes := domain.NewNode()
	colIndexes := map[string]int{
		"level_1": 0,
		"level_2": 1,
		ITEM_ID:   2,
	}

	errG, _ := errgroup.WithContext(ctx)
	errG.Go(func() error {
		return parseRow(cancelCtx, rowChan, nodes, colIndexes)
	})

	cancel() // we cancel the ctx this will cancel the parseRow context and return with error
	err := errG.Wait()
	require.ErrorContains(t, err, "context canceled")

	// restart the parseRow goroutine with the not canceled context
	errG, _ = errgroup.WithContext(ctx)
	errG.Go(func() error {
		return parseRow(ctx, rowChan, nodes, colIndexes)
	})

	rowChan <- []string{"level_1"} // send over invalid row
	err = errG.Wait()
	require.ErrorIs(t, err, errors.ErrMissingRequiredValue)

	rowChan <- []string{"level_1", "level_2", ITEM_ID} // send over valid row
	close(rowChan)

	errG, _ = errgroup.WithContext(ctx)
	errG.Go(func() error {
		return parseRow(ctx, rowChan, nodes, colIndexes)
	})

	err = errG.Wait()
	require.NoError(t, err)
	require.NotEmpty(t, nodes.Children)
	require.True(t, nodes.Children["level_1"].Children["level_2"].Children[ITEM_ID].Item, true)
}
