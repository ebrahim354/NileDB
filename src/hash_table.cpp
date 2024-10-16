#pragma once

#include <cmath>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex> 
#include <utility>
#include <vector>

template <typename K, typename V>
class Bucket {
    public:
        Bucket(size_t size, int id, int depth = 0) : size_(size), depth_(depth), id_(id) {}

        inline auto IsFull() const -> bool { return list_.size() == size_; }

        inline auto GetDepth() const -> int { return depth_; }

        inline void IncrementDepth() { depth_++; }

        inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

        inline auto GetBucketId() const -> int { return id_; }

        auto Find(const K &key, V &value) -> bool {
            for (auto it = list_.begin(); it != list_.end(); it++) {
                if (it->first == key) {
                    V v;
                    v = it->second;
                    value = v;
                    return true;
                }
            }
            return false;
        };

        auto Remove(const K &key) -> bool {
            for (auto it = list_.begin(); it != list_.end(); it++) {
                if (it->first == key) {
                    list_.erase(it);
                    return true;
                }
            }
            return false;
        }

        auto Insert(const K key, const V value) -> bool {
            if (IsFull()) {
                return false;
            }
            for (auto &it : list_) {
                if (it.first == key) {
                    it.second = value;
                    return true;
                }
            }
            list_.push_back({key, value});
            return true;
        };

    private:
        size_t size_;
        int depth_;
        int id_;
        std::list<std::pair<K, V>> list_;
};

template <typename K, typename V>
class  HashTable {
    public:
        explicit HashTable(size_t bucket_size) {
            next_bucket_id_ = 0;
            global_depth_ = 0;
            bucket_size_ = bucket_size;
            num_buckets_ = 1;
            std::shared_ptr<Bucket<K,V>> ptr(new Bucket<K,V>(bucket_size, next_bucket_id_++));
            dir_.push_back(ptr);
        }

        auto GetGlobalDepth() const -> int {
            std::lock_guard<std::mutex> lock(latch_);
            return global_depth_;
        };

        auto GetLocalDepth(int dir_index) const -> int {
            std::lock_guard<std::mutex> lock(latch_);
            return dir_[dir_index % dir_.size()]->GetDepth();
        };

        auto GetNumBuckets() const -> int {
            std::lock_guard<std::mutex> lock(latch_);
            return num_buckets_;
        }

        auto Find(const K &key, V &value) -> bool {
            std::lock_guard<std::mutex> lock(latch_);
            size_t idx = IndexOf(key);
            std::shared_ptr<Bucket<K,V>> ptr = dir_[idx];
            return ptr->Find(key, value);
        }
        void showStruct() {
            std::cout << "global depth: " << global_depth_ << std::endl;
            for (int i = 0; i < (int)dir_.size(); i++) {
                std::cout << "bucket number : " << i << " with local depth: " << dir_[i]->GetDepth() << " size "
                    << dir_[i]->GetItems().size() << " ";
                for (auto it = dir_[i]->GetItems().begin(); it != dir_[i]->GetItems().end(); it++) {
                    std::cout << "{ " << it->first << ", " << it->second << " } ";
                }
                std::cout << std::endl;
            }
        }

        void Insert(const K &key, const V &value) {
            std::lock_guard<std::mutex> lock(latch_);
            while (true) {
                size_t idx = IndexOf(key);
                //        if (key == 9) return;
                //       std::cout << "YO " << key << " " << idx << " " << bucket_size_ << std::endl;
                // std::cout << "idx: " << idx << " "
                //         << "key: " << key << std::endl;
                // showStruct();
                std::shared_ptr<Bucket<K,V>> ptr = dir_[idx];
                if (ptr->Insert(key, value)) {
                    break;
                }
                int local_depth = ptr->GetDepth();
                if (local_depth == global_depth_) {
                    size_t sz = dir_.size();
                    size_t mx = dir_.max_size();
                    if (sz * 2 > mx) {
                        return;
                    }
                    for (size_t i = 0; i < sz; i++) {
                        std::shared_ptr<Bucket<K,V>> tmp = dir_[i];
                        dir_.push_back(tmp);
                    }
                    global_depth_++;
                }
                size_t num_of_refs = pow(2, (global_depth_ - local_depth));
                size_t factor = dir_.size() / num_of_refs;
                size_t start = idx % factor;
                int new_local_depth = local_depth + 1;
                std::shared_ptr<Bucket<K,V>> new_bucket(new Bucket<K,V>(bucket_size_, next_bucket_id_++, new_local_depth));

                ptr->IncrementDepth();
                num_buckets_++;

                size_t cur = start;
                bool old = true;
                std::map<size_t, bool> mp;
                while (cur < dir_.size()) {
                    if (!old) {
                        dir_[cur] = new_bucket;
                    } else {
                        mp.insert({cur, true});
                    }
                    cur += factor;
                    old = !old;
                }

                std::list<std::pair<K, V>> *lst = &ptr->GetItems();
                for (auto it = lst->begin(); it != lst->end();) {
                    size_t cur_idx = IndexOf(it->first);
                    if (mp.count(cur_idx) != 0U) {
                        it++;
                        continue;
                    }
                    dir_[cur_idx]->Insert(it->first, it->second);
                    it = lst->erase(it);
                }
            }
        }

        auto Remove(const K &key) -> bool {
            std::lock_guard<std::mutex> lock(latch_);
            size_t idx = IndexOf(key);
            std::shared_ptr<Bucket<K,V>> ptr = dir_[idx];
            return static_cast<bool>(ptr->Remove(key));
        }


    private:

        int global_depth_;  // The global depth of the directory
        int next_bucket_id_;
        size_t bucket_size_;  // The size of a bucket
        int num_buckets_;     // The number of buckets in the hash table
        mutable std::mutex latch_;
        std::vector<std::shared_ptr<Bucket<K,V>>> dir_;  // The directory of the hash table
                                                    //
        auto GetGlobalDepthInternal() const -> int {
            return global_depth_;
        }


        auto GetNumBucketsInternal() const -> int {
            return num_buckets_;
        }

        auto IndexOf(const K &key) -> size_t {
            /*
               int mask = (1 << global_depth_) - 1;
               return std::hash<K>()(key) & mask;
               */
            size_t hashed_key = std::hash<K>()(key);
            return hashed_key % dir_.size();
        };


};

