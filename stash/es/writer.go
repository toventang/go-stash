package es

import (
	"bytes"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/kevwan/go-stash/stash/config"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	Writer struct {
		docType  string
		client   *elasticsearch.Client
		inserter *executors.ChunkExecutor
	}

	valueWithIndex struct {
		index string
		val   string
	}
)

func NewWriter(c config.ElasticSearchConf) (*Writer, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:           c.Hosts,
		Username:            c.Username,
		Password:            c.Password,
		CompressRequestBody: c.Compress,
	})
	if err != nil {
		return nil, err
	}

	writer := Writer{
		docType: c.DocType,
		client:  client,
	}
	writer.inserter = executors.NewChunkExecutor(writer.execute, executors.WithChunkBytes(c.MaxChunkBytes))
	return &writer, nil
}

func (w *Writer) Write(index, val string) error {
	return w.inserter.Add(valueWithIndex{
		index: index,
		val:   val,
	}, len(val))
}

func (w *Writer) execute(vals []interface{}) {
	var buffer bytes.Buffer
	for _, val := range vals {
		pair := val.(valueWithIndex)
		meta := fmt.Sprintf(`{"index":{"_index":"%s"}}%s`, pair.index, "\n")
		buffer.Grow(len(meta) + len(pair.val))
		buffer.WriteString(meta)
		buffer.WriteString(pair.val)
	}
	resp, err := w.client.Bulk(bytes.NewReader(buffer.Bytes()))
	if err != nil {
		logx.Error(err)
		return
	}
	defer resp.Body.Close()

	if !resp.IsError() {
		return
	}

	logx.Error(resp.Body)
}
