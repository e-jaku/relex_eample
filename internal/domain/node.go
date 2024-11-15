package domain

type Node struct {
	Item     bool             `json:"item,omitempty"`
	Children map[string]*Node `json:"children,omitempty"`
}

func (n *Node) AddNode(levels []string, itemID string) {
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
