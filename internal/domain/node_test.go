package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddNode(t *testing.T) {
	node := NewNode()
	tests := []struct {
		name         string
		levels       []string
		itemID       string
		expectedNode *Node
	}{
		{
			name:   "Add single level hierarchy",
			levels: []string{"1"},
			itemID: "1234",
			expectedNode: &Node{
				Children: map[string]*Node{
					"1": &Node{
						Children: map[string]*Node{
							"1234": &Node{
								Item: true,
							},
						},
					},
				},
			},
		},
		{
			name:   "Add 2 level hierarchy",
			levels: []string{"1", "2"},
			itemID: "4567",
			expectedNode: &Node{
				Children: map[string]*Node{
					"1": &Node{
						Children: map[string]*Node{
							"1234": &Node{
								Item: true,
							},
							"2": &Node{
								Children: map[string]*Node{
									"4567": &Node{
										Item: true,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:   "Add 3 level hierarchy",
			levels: []string{"1", "2", "3"},
			itemID: "7890",
			expectedNode: &Node{
				Children: map[string]*Node{
					"1": &Node{
						Children: map[string]*Node{
							"1234": &Node{
								Item: true,
							},
							"2": &Node{
								Children: map[string]*Node{
									"4567": &Node{
										Item: true,
									},

									"3": &Node{
										Children: map[string]*Node{
											"7890": &Node{
												Item: true,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node.AddNode(tc.levels, tc.itemID)
			require.Equal(t, tc.expectedNode, node)
		})
	}
}
