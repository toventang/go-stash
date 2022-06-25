package handler_test

import (
	"errors"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/kevwan/go-stash/stash/config"
	"github.com/kevwan/go-stash/stash/es"
	"github.com/kevwan/go-stash/stash/filter"
	"github.com/kevwan/go-stash/stash/handler"
	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

func TestConsume(t *testing.T) {
	var in = "{\"@timestamp\":\"2022-06-24T08:44:43.676Z\",\"@metadata\":{\"beat\":\"filebeat\",\"type\":\"_doc\",\"version\":\"8.1.2\"},\"agent\":{\"ephemeral_id\":\"1ca8405a-5416-44a4-be47-dbcff56f3784\",\"id\":\"c63de8d0-002c-4755-a54f-65fa1ef44b89\",\"name\":\"e6fa9d99a42e\",\"type\":\"filebeat\",\"version\":\"8.1.2\"},\"log\":{\"offset\":113903,\"file\":{\"path\":\"/var/logs/xxx/slow.log\"}},\"message\":\"{\\\"@timestamp\\\":\\\"2022-06-24T16:42:50.678+08:00\\\",\\\"level\\\":\\\"slow\\\",\\\"duration\\\":\\\"2330.7ms\\\",\\\"content\\\":\\\"[HTTP] POST - 200 - /xxx/abc - 127.0.0.1:52862 - okhttp/3.10.0 - slowcall(530.7ms)\\\",\\\"trace\\\":\\\"9ce5225bc50d218b0fbee7861d1a8394\\\",\\\"span\\\":\\\"30e31c5e5ab49040\\\"}\",\"input\":{\"type\":\"filestream\"},\"fields\":{\"app\":\"xxx\"},\"ecs\":{\"version\":\"8.0.0\"},\"host\":{\"name\":\"e6fa9d99a42e\"}}"
	h := newTestHandler()
	err := h.Consume("", in)
	assert.NoError(t, err)
	if err != nil {
		logx.Must(err)
	}
}

func newTestHandler() *handler.MessageHandler {
	c := getConfig()
	processor := c.Clusters[0]
	filters := filter.CreateFilters(processor)
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: processor.Output.ElasticSearch.Hosts,
		Username:  processor.Output.ElasticSearch.Username,
		Password:  processor.Output.ElasticSearch.Password,
	})
	logx.Must(err)

	writer, err := es.NewWriter(processor.Output.ElasticSearch)
	logx.Must(err)

	indexer := es.NewIndex(client, processor.Output.ElasticSearch.Index, time.Local)
	handle := handler.NewHandler(writer, indexer)
	handle.AddFilters(filters...)
	handle.AddFilters(filter.AddUriFieldFilter("url", "uri"))
	return handle
}

func getConfig() (c config.Config) {
	conf.MustLoad("../etc/config.yaml", &c)
	if len(c.Clusters) == 0 {
		logx.Must(errors.New("no cluster configurations"))
	}
	return c
}
