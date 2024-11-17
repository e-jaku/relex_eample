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
