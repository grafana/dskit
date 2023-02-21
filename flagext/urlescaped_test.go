package flagext

import (
	"flag"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

const (
	testS3URL        = "s3://ASDFGHJIQWETTYUI:Jkasduahdkjh213kj1h31+lkjaflkjzvKASDOasofhjafaKFAF/GoQd@region/bucket_name"
	fromWebsite      = "s3%3A%2F%2FASDFGHJIQWETTYUI%3AJkasduahdkjh213kj1h31%2BlkjaflkjzvKASDOasofhjafaKFAF%2FGoQd%40region%2Fbucket_name"
	testS3URLEscaped = "s3%3A%2F%2FASDFGHJIQWETTYUI%3AJkasduahdkjh213kj1h31%2BlkjaflkjzvKASDOasofhjafaKFAF%2FGoQd%40region%2Fbucket_name"
)

func TestURLEscaped(t *testing.T) {
	// flag
	var v URLEscaped
	flags := flag.NewFlagSet("test", flag.ExitOnError)
	flags.Var(&v, "v", "some secret credentials")
	err := flags.Parse([]string{"-v", testS3URL})
	require.NoError(t, err)
	assert.Equal(t, v.String(), testS3URLEscaped)

	// flag (but already escaped)
	err = flags.Parse([]string{"-v", testS3URLEscaped})
	require.NoError(t, err)
	assert.Equal(t, v.String(), testS3URLEscaped)

	// yaml
	yv := struct {
		S3 URLEscaped `yaml:"s3"`
	}{}
	err = yaml.Unmarshal([]byte(fmt.Sprintf("s3: %s", testS3URL)), &yv)
	require.NoError(t, err)
	assert.Equal(t, yv.S3.String(), testS3URLEscaped)

}
