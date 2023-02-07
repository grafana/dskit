package flagext

import (
	"flag"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

const (
	testS3URL = "s3://ASDFGHJIQWETTYUI:Jkasduahdkjh213kj1h31+lkjaflkjzvKASDOasofhjafaKFAF/GoQd@region/bucket_name"
)

func TestURLEscaped(t *testing.T) {
	expected, err := url.Parse(url.QueryEscape(testS3URL))
	require.NoError(t, err)

	// flag
	var v URLEscaped
	flags := flag.NewFlagSet("test", flag.ExitOnError)
	flags.Var(&v, "v", "some secret credentials")
	err = flags.Parse([]string{"-v", testS3URL})
	require.NoError(t, err)
	assert.Equal(t, v.String(), expected.String())

	// yaml
	yv := struct {
		S3 URLEscaped `yaml:"s3"`
	}{}
	err = yaml.Unmarshal([]byte(fmt.Sprintf("s3: %s", testS3URL)), &yv)
	require.NoError(t, err)
	assert.Equal(t, yv.S3.String(), expected.String())

}
