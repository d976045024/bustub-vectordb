#include "storage/index/ivfflat_index.h"
#include <algorithm>
#include <optional>
#include <random>
#include "common/exception.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {
using Vector = std::vector<double>;

IVFFlatIndex::IVFFlatIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                           VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options)
    : VectorIndex(std::move(metadata), distance_fn) {
  std::optional<size_t> lists;
  std::optional<size_t> probe_lists;
  for (const auto &[k, v] : options) {
    if (k == "lists") {
      lists = v;
    } else if (k == "probe_lists") {
      probe_lists = v;
    }
  }
  if (!lists.has_value() || !probe_lists.has_value()) {
    throw Exception("missing options: lists / probe_lists for ivfflat index");
  }
  lists_ = *lists;
  probe_lists_ = *probe_lists;
}

void VectorAdd(Vector &a, const Vector &b) {
  for (size_t i = 0; i < a.size(); i++) {
    a[i] += b[i];
  }
}

void VectorScalarDiv(Vector &a, double x) {
  for (auto &y : a) {
    y /= x;
  }
}

auto GetRandom(const std::vector<std::pair<Vector, RID>> &initial_data, const int &num_lists) -> std::vector<Vector> {
  std::vector<int> numbers(initial_data.size());
  std::vector<Vector> ans(num_lists);
  for (size_t i = 0; i < numbers.size(); i++) {
    numbers[i] = i;
  }
  std::random_device rd;
  std::mt19937 gen(rd());

  std::shuffle(numbers.begin(), numbers.end(), gen);
  for (int i = 0; i < num_lists; i++) {
    ans[i] = initial_data[numbers[i]].first;
  }
  return ans;
}

// Find the nearest centroid to the base vector in all centroids
auto FindCentroid(const Vector &vec, const std::vector<Vector> &centroids, VectorExpressionType dist_fn) -> size_t {
  double min_dist = -1;
  int min_id = -1;
  
  for (size_t i = 0; i < centroids.size(); i++) {
    double dist = ComputeDistance(vec, centroids[i], dist_fn);
    if (min_dist < 0 || dist < min_dist) {
      min_dist = dist;
      min_id = i;
    }
  }
  return min_id;
}

// Compute new centroids based on the original centroids.
auto FindCentroids(const std::vector<std::pair<Vector, RID>> &data, const std::vector<Vector> &centroids,
                   VectorExpressionType dist_fn, std::vector<std::vector<std::pair<Vector, RID>>> &centroids_buckets) -> std::vector<Vector> {
  std::vector<std::vector<std::pair<Vector, RID>>> buckets(centroids.size());
  for (const std::pair<Vector, RID> &vec : data) {
    size_t centroid = FindCentroid(vec.first, centroids, dist_fn);
    buckets[centroid].push_back(vec);
  }
  std::vector<Vector> new_centroids(centroids.size());
  int id = 0;
  for (const std::vector<std::pair<Vector, RID>> &bucket : buckets) {
    Vector vec = bucket[0].first;
    for (size_t i = 1; i < bucket.size(); i++) {
      VectorAdd(vec, bucket[i].first);
    }
    VectorScalarDiv(vec, bucket.size());
    new_centroids[id++] = vec;
  }
  centroids_buckets = buckets;
  return new_centroids;
}

void IVFFlatIndex::BuildIndex(std::vector<std::pair<Vector, RID>> initial_data) {
  if (initial_data.empty()) {
    return;
  }
  centroids_ = GetRandom(initial_data, lists_);
  centroids_ = FindCentroids(initial_data, centroids_, distance_fn_, centroids_buckets_);
  // IMPLEMENT ME
}

void IVFFlatIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  auto centroid = FindCentroid(key, centroids_, distance_fn_);
  centroids_buckets_[centroid].push_back({key, rid});
}

auto IVFFlatIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> {
  std::vector<int> numbers(centroids_buckets_.size());
  for (size_t i = 0; i < numbers.size(); i++) {
    numbers[i] = i;
  }
  std::sort(numbers.begin(), numbers.end(), [&](const int &i, const int &j) {
    return ComputeDistance(centroids_[i], base_vector, distance_fn_) < ComputeDistance(centroids_[j], base_vector, distance_fn_);
  });
  std::vector<std::pair<Vector, RID>> vecs;
  for (size_t i = 0; i < std::min(probe_lists_, numbers.size()); i++) {
    for (const std::pair<Vector, RID> &p : centroids_buckets_[numbers[i]]) {
      vecs.push_back(p);
    }
  }
  std::sort(vecs.begin(), vecs.end(), [&](const std::pair<Vector, RID> &p1, const std::pair<Vector, RID> &p2) {
    return ComputeDistance(p1.first, base_vector, distance_fn_) < ComputeDistance(p2.first, base_vector, distance_fn_);
  });
  std::vector<RID> ans;
  for (size_t i = 0; i < std::min(vecs.size(), limit); i++) {
    ans.push_back(vecs[i].second);
  }
  return ans;
}

}  // namespace bustub
