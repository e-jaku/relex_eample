package domain

import "sync"

type Node struct {
	mu       sync.Mutex       `json:"-"`
	Item     bool             `json:"item,omitempty"`
	Children map[string]*Node `json:"children,omitempty"`
}

func NewNode() *Node {
	return &Node{
		mu: sync.Mutex{},
	}
}

// AddNode adds to the existing node hierarchy a new item Node.
// It iterates the depth levels specified in the levels []string and adds the item Node at the deepest point.
// If in-between levels are missing this method creates them.
// Adding to Node hierarchy is guarded via lock, and therefore threadsafe.
func (n *Node) AddNode(levels []string, itemID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	current := n
	for _, level := range levels {
		if current.Children == nil {
			current.Children = make(map[string]*Node)
		}

		if _, exists := current.Children[level]; !exists {
			current.Children[level] = &Node{}
		}

		current = current.Children[level]
	}

	if current.Children == nil {
		current.Children = make(map[string]*Node)
	}
	current.Children[itemID] = &Node{Item: true}
}
