package kubernetes

import (
	"encoding/base64"
	"encoding/json"
)

type operation struct {
	Op   string `json:"op"`
	Path string `json:"path"`
	// Value should be a base64-encoded value or nil.
	Value *string `json:"value"`
}

// preparePatch prepares a JSON patch (RFC 6902) to update a configmap key. If oldHash is empty or nil
// this is equivalent to adding a key.
func preparePatch(key string, oldHash, newVal, newHash []byte) ([]byte, error) {
	hashKey := "/binaryData/" + convertKeyToStoreHash(key)

	b64 := func(b []byte) *string {
		str := base64.StdEncoding.EncodeToString(b)
		return &str
	}

	// testing with nil is equivalent to testing that the key doesn't exist
	var expectedHash *string
	if len(oldHash) > 0 {
		expectedHash = b64(oldHash)
	}

	testHashOp := operation{
		Op:    "test",
		Path:  hashKey,
		Value: expectedHash,
	}

	setHashOp := operation{
		Op:    "replace",
		Path:  hashKey,
		Value: b64(newHash),
	}

	setDataOp := operation{
		Op:    "replace",
		Path:  "/binaryData/" + convertKeyToStore(key),
		Value: b64(newVal),
	}

	patch := []operation{testHashOp, setHashOp, setDataOp}

	return json.Marshal(patch)
}

func prepareDeletePatch(key string) ([]byte, error) {
	removeHashOp := operation{
		Op:    "remove",
		Path:  "/binaryData/" + convertKeyToStoreHash(key),
		Value: nil,
	}

	removeObjectOp := operation{
		Op:    "remove",
		Path:  "/binaryData/" + convertKeyToStore(key),
		Value: nil,
	}

	patch := []operation{removeHashOp, removeObjectOp}

	return json.Marshal(patch)
}
