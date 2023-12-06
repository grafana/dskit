package pageidparser

import (
	"embed"
	"encoding/json"
	"fmt"

	"github.com/AlessandroPomponio/go-gibberish/gibberish"
	"github.com/AlessandroPomponio/go-gibberish/structs"
	lru "github.com/hashicorp/golang-lru/v2"
)

var classifier *structs.GibberishData

const maxSegments = 10

var words, _ = lru.New[string, bool](8192)

//go:embed classifier.json
var dataFile embed.FS

func loadKnowledgeBase() (*structs.GibberishData, error) {
	content, err := dataFile.ReadFile("classifier.json")
	if err != nil {
		return nil, fmt.Errorf("LoadKnowledgeBase: unable to read knowledge base content: %w", err)
	}

	var data structs.GibberishData
	err = json.Unmarshal(content, &data)
	if err != nil {
		return nil, fmt.Errorf("LoadKnowledgeBase: unable to unmarshal knowledge base content: %w", err)
	}

	return &data, nil
}

func InitAutoClassifier() error {
	var err error
	classifier, err = loadKnowledgeBase()
	if err != nil {
		return err
	}

	return nil
}

// This function takes a path and returns a "clustered" path, where
// all the "IDs" in the path are replaced by a single "*" character.
// For example, the path "/foo/42/baz" would be replaced with "/foo/*/baz".
// The purpose of this function is to allow for a large number of paths
// to be grouped into a smaller number of paths.

//nolint:cyclop
func ClusterURL(path string) string {
	if path == "" {
		return path
	}

	p := []byte(path)
	sPos := 0
	sFwd := 0

	skip := false
	skipGrace := true
	nSegments := 0
	for _, c := range p {
		if c == '/' {
			nSegments++
			if skip {
				p[sPos] = '*'
				sPos++
			} else if sFwd > sPos {
				if !okWord(string(p[sPos:sFwd])) {
					p[sPos] = '*'
					sPos++
				} else {
					sPos = sFwd
				}
			}

			if nSegments >= maxSegments {
				break
			}

			p[sPos] = '/'
			sPos++
			sFwd = sPos
			skip = false
			skipGrace = true
		} else if !skip {
			p[sFwd] = c
			sFwd++
			if !isAlpha(c) {
				if skipGrace && (sFwd-sPos) == 2 {
					skipGrace = false
					continue
				}
				skip = true
			}
		}
	}

	if skip {
		p[sPos] = '*'
		sPos++
	} else if sFwd > sPos {
		if !okWord(string(p[sPos:sFwd])) {
			p[sPos] = '*'
			sPos++
		} else {
			sPos = sFwd
		}
	}

	return string(p[:sPos])
}

func okWord(w string) bool {
	_, ok := words.Get(w)
	if ok {
		return ok
	}
	if gibberish.IsGibberish(w, classifier) {
		return false
	}

	words.Add(w, true)
	return true
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '-' || c == '_'
}
