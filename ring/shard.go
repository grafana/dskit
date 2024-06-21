package ring

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

const (
	datasetsDir                      = "datasets"
	realRingDescJsonFile             = "realRingDesc.json"
	maxGlobalSeriesPerTenantJsonFile = "maxGlobalSeriesPerTenant.json"
	shardSizeByTenantJsonFile        = "shardSizeByTenant.json"
	timeSeriesCountByTenantJsonFile  = "timeSeriesCountByTenant.json"
	defaultMaxGlobalLimit            = 150000
)

type metric struct {
	User string `json:"user"`
}

type jsonEntry struct {
	Metric metric `json:"metric"`
	Value  []any  `json:"value"`
}

type jsonEntries struct {
	Description string       `json:"description"`
	Results     []*jsonEntry `json:"results"`
}

func GetRealRingDesc(dir string, prefix string) (*Desc, error) {
	filePath := realRingDescJsonFile
	if prefix != "" {
		filePath = fmt.Sprintf("%s-%s", prefix, filePath)
	}

	if dir == "" {
		filePath = path.Join(datasetsDir, filePath)
	} else {
		filePath = path.Join(dir, datasetsDir, filePath)
	}
	buffer, err := loadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load real ring desc json from "+realRingDescJsonFile)
	}
	return processRingDescJson(buffer)
}

func GetMaxGlobalLimitByTenantID(dir string, prefix string) (map[string]int, error) {
	filePath := maxGlobalSeriesPerTenantJsonFile
	if prefix != "" {
		filePath = fmt.Sprintf("%s-%s", prefix, filePath)
	}
	if dir == "" {
		filePath = path.Join(datasetsDir, filePath)
	} else {
		filePath = path.Join(dir, datasetsDir, filePath)
	}
	buffer, err := loadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load tenant shard max global series json from "+maxGlobalSeriesPerTenantJsonFile)
	}
	maxGlobalSeriesByTenantId, err := processJson(buffer)
	if err != nil {
		return nil, err
	}

	shardSizeByTenantId, err := GetShardSizeByTenantID(dir, prefix)
	if err != nil {
		return nil, err
	}

	for tenantId := range shardSizeByTenantId {
		if _, ok := maxGlobalSeriesByTenantId[tenantId]; !ok {
			maxGlobalSeriesByTenantId[tenantId] = defaultMaxGlobalLimit
		}
	}
	return maxGlobalSeriesByTenantId, nil
}

func GetShardSizeByTenantID(dir string, prefix string) (map[string]int, error) {
	filePath := shardSizeByTenantJsonFile
	if prefix != "" {
		filePath = fmt.Sprintf("%s-%s", prefix, filePath)
	}

	if dir == "" {
		filePath = path.Join(datasetsDir, filePath)
	} else {
		filePath = path.Join(dir, datasetsDir, filePath)
	}
	buffer, err := loadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load tenant shard size json from "+shardSizeByTenantJsonFile)
	}
	return processJson(buffer)
}

func GetTimeseriesCountByTenantID(dir string, prefix string) (map[string]int, error) {
	filePath := timeSeriesCountByTenantJsonFile
	if prefix != "" {
		filePath = fmt.Sprintf("%s-%s", prefix, filePath)
	}
	if dir == "" {
		filePath = path.Join(datasetsDir, filePath)
	} else {
		filePath = path.Join(dir, datasetsDir, filePath)
	}
	buffer, err := loadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load tenant time series timeseriesCount json from "+timeSeriesCountByTenantJsonFile)
	}
	return processJson(buffer)
}

func processJson(buffer []byte) (map[string]int, error) {
	var entries jsonEntries
	err := json.Unmarshal(buffer, &entries)
	if err != nil {
		return nil, err
	}
	dataByTenantID := make(map[string]int, len(entries.Results))
	for _, entry := range entries.Results {
		value, ok := entry.Value[1].(string)
		if !ok {
			continue
		}
		shardSize, err := strconv.Atoi(value)
		if err != nil {
			continue
		}
		dataByTenantID[entry.Metric.User] = shardSize
	}
	return dataByTenantID, nil
}

func processRingDescJson(buffer []byte) (*Desc, error) {
	var instanceDescs []InstanceDesc
	err := json.Unmarshal(buffer, &instanceDescs)
	if err != nil {
		return nil, err
	}
	instances := make(map[string]InstanceDesc, len(instanceDescs))
	now := time.Now().Unix()
	for _, instanceDesc := range instanceDescs {
		instanceDesc.Timestamp = now
		instanceDesc.State = ACTIVE
		instanceDesc.Id = instanceDesc.Addr
		instances[instanceDesc.Addr] = instanceDesc
	}
	return &Desc{Ingesters: instances}, nil
}

func loadFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)

	_, err = file.Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}
