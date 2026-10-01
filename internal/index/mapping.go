// Package index owns the search index: its mapping, and the write path that
// turns a batch of document events into versioned, conflict-checked writes.
package index

// Mapping returns the index body for the documents index. dims is the
// embedding size; refresh is the refresh interval (how quickly an
// acknowledged write becomes searchable).
//
// The mapping is strict, so a typo in a field name fails loudly instead of
// silently creating an unmapped field. Permission fields are keywords and are
// only ever used in filters: they never affect scoring.
func Mapping(dims int, refresh string) map[string]any {
	keyword := map[string]any{"type": "keyword"}
	long := map[string]any{"type": "long"}
	boolean := map[string]any{"type": "boolean"}
	date := map[string]any{"type": "date"}
	text := map[string]any{"type": "text", "analyzer": "ss_text"}

	return map[string]any{
		"settings": map[string]any{
			"index": map[string]any{
				"number_of_shards":   1,
				"number_of_replicas": 0,
				"refresh_interval":   refresh,
				"knn":                true,
			},
			"analysis": map[string]any{
				"analyzer": map[string]any{
					"ss_text": map[string]any{
						"type":      "custom",
						"tokenizer": "standard",
						"filter":    []string{"lowercase", "asciifolding", "porter_stem"},
					},
				},
			},
		},
		"mappings": map[string]any{
			"dynamic": "strict",
			"properties": map[string]any{
				"id":         keyword,
				"datasource": keyword,
				"container":  keyword,
				"kind":       keyword,
				"title": map[string]any{
					"type": "text", "analyzer": "ss_text",
					"fields": map[string]any{"raw": map[string]any{"type": "keyword", "ignore_above": 256}},
				},
				"body":              text,
				"url":               map[string]any{"type": "keyword", "index": false},
				"author":            keyword,
				"parent_id":         keyword,
				"labels":            keyword,
				"created_at":        date,
				"updated_at":        date,
				"allowed":           keyword,
				"version":           long,
				"src_ts_ms":         long,
				"mode":              keyword,
				"emit_ts_ms":        long,
				"indexed_at_ms":     long,
				"content_hash":      keyword,
				"embed_hash":        keyword,
				"needs_embed":       boolean,
				"deleted":           boolean,
				"quarantined":       boolean,
				"quarantined_at_ms": long,
				"embedding": map[string]any{
					"type":      "knn_vector",
					"dimension": dims,
					"method": map[string]any{
						"name":       "hnsw",
						"engine":     "lucene",
						"space_type": "cosinesimil",
						"parameters": map[string]any{"m": 16, "ef_construction": 128},
					},
				},
			},
		},
	}
}
