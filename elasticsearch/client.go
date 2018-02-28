package ElasticSearch

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"math"
)

// Client allows sending batches of Prometheus samples to ElasticSearch.
type Client struct {
	url       string
	index     string
	indexType string
}

// NewClient creates a new Client.
func NewClient(url string, index string, indexType string) *Client {
	return &Client{
		url:       url,
		index:     index,
		indexType: indexType,
	}
}

// ElasticSearchSamplesRequest is used for building a JSON request for storing samples
// via the ElasticSearch.
type ElasticSearchSamplesRequest struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

// tagsFromMetric translates Prometheus metric into ElasticSearch tags.
func tagsFromMetric(m model.Metric) map[string]string {
	tags := make(map[string]string, len(m)-1)
	for l, v := range m {
		if l == model.MetricNameLabel {
			continue
		}
		tags[string(l)] = string(v)
	}
	return tags
}

// Write sends a batch of samples to ElasticSearch.
func (c *Client) Write(samples model.Samples) error {

	client, err := elastic.NewClient(elastic.SetURL(c.url))
	if err != nil {
		return err
	}

	client.Start()
	defer client.Stop()

	bulkRequest := client.Bulk()

	for _, s := range samples {
		v := float64(s.Value)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			log.Warnf("cannot send value %f to ElasticSearch, skipping sample %#v", v, s)
			continue
		}
		metric := string(s.Metric[model.MetricNameLabel])
		doc := ElasticSearchSamplesRequest{
			Metric:    metric,
			Timestamp: s.Timestamp.Unix(),
			Value:     v,
			Tags:      tagsFromMetric(s.Metric),
		}

		indexRequest := elastic.NewBulkIndexRequest().Index(c.index).Type(c.indexType).Doc(doc)
		if err != nil {
			return err
		}

		bulkRequest = bulkRequest.Add(indexRequest)
	}

	bulkResponse, err := bulkRequest.Do(context.TODO())

	if err != nil {
		fmt.Errorf("failed to write the samples to ElasticSearch,", err)
	}
	if bulkResponse == nil {
		fmt.Errorf("expected bulkResponse to be != nil; got nil")
	}

	if bulkRequest.NumberOfActions() != 0 {
		fmt.Errorf("expected bulkRequest.NumberOfActions %d; got %d", 0, bulkRequest.NumberOfActions())
	}

	return nil
}

func (c Client) Name() string {
	return "ElasticSearch"
}
