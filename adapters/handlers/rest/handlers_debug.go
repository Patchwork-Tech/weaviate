//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/schema"
)

func setupDebugHandlers(appState *state.State) {
	logger := appState.Logger.WithField("handler", "debug")

	http.HandleFunc("/debug/index/rebuild/vector", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.Enabled(os.Getenv("ASYNC_INDEXING")) {
			http.Error(w, "async indexing is not enabled", http.StatusNotImplemented)
			return
		}

		colName := r.URL.Query().Get("collection")
		shardName := r.URL.Query().Get("shard")
		targetVector := r.URL.Query().Get("vector")

		if colName == "" || shardName == "" {
			http.Error(w, "collection and shard are required", http.StatusBadRequest)
			return
		}

		idx := appState.DB.GetIndex(schema.ClassName(colName))
		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found")
			http.Error(w, "collection not found", http.StatusNotFound)
			return
		}

		err := idx.DebugResetVectorIndex(context.Background(), shardName, targetVector)
		if err != nil {
			logger.
				WithField("shard", shardName).
				WithField("targetVector", targetVector).
				WithError(err).
				Error("failed to reset vector index")
			if errTxt := err.Error(); strings.Contains(errTxt, "not found") {
				http.Error(w, "shard not found", http.StatusNotFound)
			}

			http.Error(w, "failed to reset vector index", http.StatusInternalServerError)
			return
		}

		logger.Info("reindexing started", zap.String("shard", shardName), zap.String("collection", colName), zap.String("targetVector", targetVector))

		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/index/repair/vector", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.Enabled(os.Getenv("ASYNC_INDEXING")) {
			http.Error(w, "async indexing is not enabled", http.StatusNotImplemented)
			return
		}

		colName := r.URL.Query().Get("collection")
		shardName := r.URL.Query().Get("shard")
		targetVector := r.URL.Query().Get("vector")

		if colName == "" || shardName == "" {
			http.Error(w, "collection and shard are required", http.StatusBadRequest)
			return
		}

		idx := appState.DB.GetIndex(schema.ClassName(colName))
		if idx == nil {
			logger.Error("Failed to retrieve collection", zap.String("collection", colName))
			http.Error(w, "collection not found", http.StatusNotFound)
			return
		}

		err := idx.DebugRepairIndex(context.Background(), shardName, targetVector)
		if err != nil {
			logger.
				WithField("shard", shardName).
				WithField("targetVector", targetVector).
				WithError(err).
				Error("failed to repair vector index")
			if errTxt := err.Error(); strings.Contains(errTxt, "not found") {
				http.Error(w, "shard not found", http.StatusNotFound)
			}

			http.Error(w, "failed to repair vector index", http.StatusInternalServerError)
			return
		}

		logger.
			WithField("shard", shardName).
			WithField("targetVector", targetVector).
			Info("repair started")

		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/stats/collection/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/debug/stats/collection/"))
		parts := strings.Split(path, "/")
		if len(parts) < 3 || len(parts) > 5 || parts[1] != "shards" {
			logger.Warn("Invalid path in debug stats collection request", zap.Strings("parts", parts), zap.String("path", path))
			http.Error(w, "invalid path", http.StatusNotFound)
			return
		}

		colName, shardName := parts[0], parts[2]
		vecIdxID := "main"
		if len(parts) == 4 {
			vecIdxID = parts[3]
		}

		idx := appState.DB.GetIndex(schema.ClassName(colName))
		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found")
			http.Error(w, "collection not found", http.StatusNotFound)
			return
		}

		shard := idx.GetShard(shardName)
		if shard == nil {
			logger.Warn("Failed to retrieve shard for collection statistics", zap.String("collection", colName), zap.String("shard", shardName), zap.String("vectorIndexID", vecIdxID), zap.Strings("pathParts", parts))
			http.Error(w, "shard not found", http.StatusNotFound)
			return
		}

		// Get the vector index
		var vidx db.VectorIndex
		if vecIdxID == "main" {
			vidx = shard.VectorIndex()
		} else {
			vidx = shard.VectorIndexes()[vecIdxID]
		}

		if vidx == nil {
			logger.Error("Failed to find vector index", zap.String("shard", shardName), zap.String("collection", colName), zap.String("vectorIndexID", vecIdxID))
			http.Error(w, "vector index not found", http.StatusNotFound)
			return
		}

		stats, err := vidx.Stats()
		if err != nil {
			logger.Error("Failed to get vector index stats", zap.String("collection", colName), zap.String("shard", shardName), zap.String("vectorIndexID", vecIdxID), zap.Error(err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		jsonBytes, err := json.Marshal(stats)
		if err != nil {
			logger.Error("Failed to marshal stats for vector index", zap.String("path", r.URL.Path), zap.String("collection", colName), zap.String("shard", shardName), zap.String("vectorIndexID", vecIdxID), zap.Error(err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		logger.Info("HNSW stats retrieval successful", zap.String("path", r.URL.Path), zap.String("collection", colName), zap.String("shard", shardName), zap.String("vectorIndexID", vecIdxID), zap.Int("statCount", stats.Count()), zap.Int("statDim", stats.Dim()))

		w.WriteHeader(http.StatusOK)
		w.Write(jsonBytes)
	}))
}
