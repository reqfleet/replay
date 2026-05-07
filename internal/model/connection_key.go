package model

import (
	"fmt"
	"strconv"
	"strings"
)

type ConnectionKey struct {
	Node         string
	ConnectionID int
}

func (k ConnectionKey) MarshalText() ([]byte, error) {
	if k.Node == "" {
		return []byte(strconv.Itoa(k.ConnectionID)), nil
	}
	return []byte(k.Node + "\x00" + strconv.Itoa(k.ConnectionID)), nil
}

func (k *ConnectionKey) UnmarshalText(text []byte) error {
	raw := string(text)
	if raw == "" {
		return fmt.Errorf("empty connection key")
	}
	if node, connectionIDText, ok := strings.Cut(raw, "\x00"); ok {
		connectionID, err := strconv.Atoi(connectionIDText)
		if err != nil {
			return fmt.Errorf("parse connection key %q: %w", raw, err)
		}
		k.Node = node
		k.ConnectionID = connectionID
		return nil
	}
	connectionID, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("parse connection key %q: %w", raw, err)
	}
	k.Node = ""
	k.ConnectionID = connectionID
	return nil
}
