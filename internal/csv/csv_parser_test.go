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
	str := `level_1,level_2,level_3,item_id
1,12,103,12507622
1,13,,32622917
`
	reader := strings.NewReader(str)
	parse := &CsvParser{}

	hierarchy, err := parse.ParseFile(context.Background(), reader)
	require.NoError(t, err)
	require.NotNil(t, hierarchy)
}

func TestValidateColIndexes(t *testing.T) {
	tests := []struct {
		name              string
		expectedErrorType error
		containsErrMsg    string
		colIndexes        columnIndexes
	}{
		{
			name:              "Missing LV1 col",
			expectedErrorType: errors.ErrMissingRequiredColumn,
			colIndexes: columnIndexes{
				LV1Index: -1,
			},
			containsErrMsg: "missing col level_1",
		},
		{
			name:              "Missing Item ID col",
			expectedErrorType: errors.ErrMissingRequiredColumn,
			colIndexes: columnIndexes{
				ItemIDIndex: -1,
			},
			containsErrMsg: "missing col item_id",
		},
		{
			name:              "Missing Parent col index",
			expectedErrorType: errors.ErrMissingRequiredColumn,
			colIndexes: columnIndexes{
				LV2Index: -1,
			},
			containsErrMsg: "missing col level_2 required due to presence of level_3",
		},
		{
			name: "Valid case",
			colIndexes: columnIndexes{
				LV1Index:    0,
				LV2Index:    1,
				LV3Index:    2,
				ItemIDIndex: 3,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateColIndexes(tc.colIndexes)
			if tc.expectedErrorType == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.expectedErrorType)
				require.ErrorContains(t, err, tc.containsErrMsg)
			}
		})
	}
}

func TestExtractColIndexes(t *testing.T) {
	tests := []struct {
		name               string
		expectedErrorType  error
		columns            []string
		expectedColIndexes columnIndexes
	}{
		{
			name:    "Valid case with missing LV3",
			columns: []string{LV1, LV2, ITEM_ID},
			expectedColIndexes: columnIndexes{
				LV1Index:    0,
				LV2Index:    1,
				LV3Index:    -1,
				ItemIDIndex: 2,
			},
		},
		{
			name:    "Valid case with all LV",
			columns: []string{LV1, LV2, LV3, ITEM_ID},
			expectedColIndexes: columnIndexes{
				LV1Index:    0,
				LV2Index:    1,
				LV3Index:    2,
				ItemIDIndex: 3,
			},
		},
		{
			name:    "Valid case with different order",
			columns: []string{LV2, LV3, ITEM_ID, LV1},
			expectedColIndexes: columnIndexes{
				LV1Index:    3,
				LV2Index:    0,
				LV3Index:    1,
				ItemIDIndex: 2,
			},
		},
		{
			name:              "Unknown type",
			columns:           []string{LV1, LV2, ITEM_ID, "other_unknown_column"},
			expectedErrorType: errors.ErrUnknownColumn,
		},
		{
			name:              "Wrong Reoccurring column",
			columns:           []string{LV1, LV2, LV1},
			expectedErrorType: errors.ErrReoccurringColumn,
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
		colIndexes        columnIndexes
		expectedLevels    []string
	}{
		{
			name:           "Valid case",
			row:            []string{LV1, LV2, ITEM_ID},
			expectedLevels: []string{LV1, LV2},
			colIndexes: columnIndexes{
				LV1Index:    0,
				LV2Index:    1,
				LV3Index:    -1,
				ItemIDIndex: 2,
			},
		},
		{
			name:           "Valid case with mixed order",
			row:            []string{LV3, LV1, LV2, ITEM_ID},
			expectedLevels: []string{LV1, LV2, LV3},
			colIndexes: columnIndexes{
				LV1Index:    1,
				LV2Index:    2,
				LV3Index:    0,
				ItemIDIndex: 3,
			},
		},
		{
			name: "Invalid row",
			row:  []string{"some random data"},
			colIndexes: columnIndexes{
				LV1Index:    0,
				LV2Index:    2,
				LV3Index:    1,
				ItemIDIndex: 3,
			},
			expectedErrorType: errors.ErrMissingRequiredValue,
		},
		{
			name: "Invalid row, missing parent to child element",
			row:  []string{LV1, "", LV3, ITEM_ID},
			colIndexes: columnIndexes{
				LV1Index:    0,
				LV2Index:    1,
				LV3Index:    2,
				ItemIDIndex: 3,
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
	colIndexes := columnIndexes{
		LV1Index:    0,
		LV2Index:    1,
		LV3Index:    -1,
		ItemIDIndex: 2,
	}

	errG, _ := errgroup.WithContext(ctx)
	errG.Go(func() error {
		return parseRow(cancelCtx, rowChan, nodes, colIndexes)
	})

	cancel() // we cancel the ctx this will cancel the parseRow context and return with error
	err := errG.Wait()
	require.ErrorContains(t, err, "context canceled")

	// Restart the parseRow goroutine with the not canceled context
	errG, _ = errgroup.WithContext(ctx)
	errG.Go(func() error {
		return parseRow(ctx, rowChan, nodes, colIndexes)
	})

	rowChan <- []string{LV1} // send over invalid row
	err = errG.Wait()
	require.ErrorIs(t, err, errors.ErrMissingRequiredValue)

	rowChan <- []string{LV1, LV2, ITEM_ID} // send over valid last row
	close(rowChan)

	errG, _ = errgroup.WithContext(ctx)
	errG.Go(func() error {
		return parseRow(ctx, rowChan, nodes, colIndexes)
	})

	err = errG.Wait()
	require.NoError(t, err)
	require.NotEmpty(t, nodes.Children)
	require.True(t, nodes.Children[LV1].Children[LV2].Children[ITEM_ID].Item, true)
}
