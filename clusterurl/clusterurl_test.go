package clusterurl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterURL(t *testing.T) {
	err := InitAutoClassifier()
	assert.NoError(t, err)
	assert.Equal(t, "", ClusterURL(""))
	assert.Equal(t, "/users/*/j4elk/*/job/*", ClusterURL("/users/fdklsd/j4elk/23993/job/2"))
	assert.Equal(t, "*", ClusterURL("123"))
	assert.Equal(t, "/*", ClusterURL("/123"))
	assert.Equal(t, "*/", ClusterURL("123/"))
	assert.Equal(t, "*/*", ClusterURL("123/ljgdflgjf"))
	assert.Equal(t, "/*", ClusterURL("/**"))
	assert.Equal(t, "/u/*", ClusterURL("/u/2"))
	assert.Equal(t, "/v1/products/*", ClusterURL("/v1/products/2"))
	assert.Equal(t, "/v1/products/*", ClusterURL("/v1/products/22"))
	assert.Equal(t, "/v1/products/*", ClusterURL("/v1/products/22j"))
	assert.Equal(t, "/products/*/org/*", ClusterURL("/products/1/org/3"))
	assert.Equal(t, "/products//org/*", ClusterURL("/products//org/3"))
	assert.Equal(t, "/v1/k6-test-runs/*", ClusterURL("/v1/k6-test-runs/1"))
	assert.Equal(t, "/attach", ClusterURL("/attach"))
	assert.Equal(t, "/usuarios/*/j4elk/*/trabajo/*", ClusterURL("/usuarios/fdklsd/j4elk/23993/trabajo/2"))
	assert.Equal(t, "/Benutzer/*/j4elk/*/Arbeit/*", ClusterURL("/Benutzer/fdklsd/j4elk/23993/Arbeit/2"))
	assert.Equal(t, "/utilisateurs/*/j4elk/*/tache/*", ClusterURL("/utilisateurs/fdklsd/j4elk/23993/tache/2"))
	assert.Equal(t, "/products/", ClusterURL("/products/"))
	assert.Equal(t, "/user-space/", ClusterURL("/user-space/"))
	assert.Equal(t, "/user_space/", ClusterURL("/user_space/"))
}
