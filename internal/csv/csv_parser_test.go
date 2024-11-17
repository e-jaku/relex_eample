package csv

import (
	"context"
	"strings"
	"testing"

	"github.com/formulatehq/data-engineer/internal/errors"
	"github.com/stretchr/testify/require"
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
			containsErrMsg: "missing col level_2 due to presence of level_3",
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
