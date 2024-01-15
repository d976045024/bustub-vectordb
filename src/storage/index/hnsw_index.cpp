#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <queue>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/macros.h"
#include "execution/expressions/vector_expression.h"
#include "fmt/format.h"
#include "fmt/std.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {
HNSWIndex::HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                     VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options)
    : VectorIndex(std::move(metadata), distance_fn),
      vertices_(std::make_unique<std::vector<Vector>>()),
      layers_{{*vertices_, distance_fn}} {
  std::optional<size_t> m;
  std::optional<size_t> ef_construction;
  std::optional<size_t> ef_search;
  for (const auto &[k, v] : options) {
    if (k == "m") {
      m = v;
    } else if (k == "ef_construction") {
      ef_construction = v;
    } else if (k == "ef_search") {
      ef_search = v;
    }
  }
  if (!m.has_value() || !ef_construction.has_value() || !ef_search.has_value()) {
    throw Exception("missing options: m / ef_construction / ef_search for hnsw index");
  }
  ef_construction_ = *ef_construction;
  m_ = *m;
  ef_search_ = *ef_search;
}

auto NSW::FindNearestNeighbors(const std::vector<double> &base_vector, size_t limit, size_t entry_point)
    -> std::vector<size_t> {
  BUSTUB_ASSERT(limit > 0, "limit > 0");
  std::vector<size_t> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<double, size_t>, std::vector<std::pair<double, size_t>>, std::greater<>> explore_q;
  std::priority_queue<std::pair<double, size_t>, std::vector<std::pair<double, size_t>>, std::less<>> result_set;
  auto dist = ComputeDistance(vertices_[entry_point], base_vector, dist_fn_);
  explore_q.emplace(dist, entry_point);
  result_set.emplace(dist, entry_point);
  visited.emplace(entry_point);
  while (!explore_q.empty()) {
    auto [dist, vertex] = explore_q.top();
    explore_q.pop();
    if (dist > result_set.top().first) {
      break;
    }
    for (const auto &neighbor : edges_[vertex]) {
      if (visited.find(neighbor) == visited.end()) {
        visited.emplace(neighbor);
        auto dist = ComputeDistance(vertices_[neighbor], base_vector, dist_fn_);
        explore_q.emplace(dist, neighbor);
        result_set.emplace(dist, neighbor);
        while (result_set.size() > limit) {
          result_set.pop();
        }
      }
    }
  }
  while (!result_set.empty()) {
    candidates.push_back(result_set.top().second);
    result_set.pop();
  }
  std::reverse(candidates.begin(), candidates.end());
  return candidates;
}

auto NSW::AddVertex(size_t vertex_id) { in_vertices_.push_back(vertex_id); }

auto NSW::Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m) {
  if (!in_vertices_.empty()) {
    auto neighbors = FindNearestNeighbors(vec, m, in_vertices_[0]);
    for (size_t i = 0; i < m && i < neighbors.size(); i++) {
      Connect(vertex_id, neighbors[i]);
    }
  }
  AddVertex(vertex_id);
}

void NSW::Connect(size_t vertex_a, size_t vertex_b) {
  edges_[vertex_a].push_back(vertex_b);
  edges_[vertex_b].push_back(vertex_a);
}

auto HNSWIndex::AddVertex(const std::vector<double> &vec, RID rid) -> size_t {
  auto id = vertices_->size();
  vertices_->emplace_back(vec);
  rids_.emplace_back(rid);
  return id;
}

void HNSWIndex::BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) {
  std::random_device rand_dev;
  std::mt19937 generator(rand_dev());
  std::shuffle(initial_data.begin(), initial_data.end(), generator);

  for (const auto &[vec, rid] : initial_data) {
    auto id = AddVertex(vec, rid);
    layers_[0].Insert(vec, id, ef_construction_, m_);
  }
}

auto HNSWIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> {
  auto vertex_ids = layers_[0].FindNearestNeighbors(base_vector, limit, layers_[0].in_vertices_[0]);
  std::vector<RID> result;
  result.reserve(vertex_ids.size());
  for (const auto &id : vertex_ids) {
    result.push_back(rids_[id]);
  }
  return result;
}

void HNSWIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  auto id = AddVertex(key, rid);
  layers_[0].Insert(key, id, ef_construction_, m_);
}

}  // namespace bustub
