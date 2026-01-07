#pragma once

#include <array>
#include <cstdint>
#include <iostream>
#include <map>
#include <mutex> 
#include <unordered_map>
#include <utility>
#include <cassert>
#include "lru_k_replacer.h"



LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    // std::cout << "replacer_size_: " << replacer_size_ << " k: " << k << std::endl;
}

auto LRUKReplacer::Size() {
    const std::lock_guard<std::mutex> lock(latch_);
    // std::cout << "Size" << std::endl;
    return cur_size_;
}
// for debuging.
void LRUKReplacer::Show() {
    for (auto it:frames_) {
        std::cout << "frame id: " << it.second;
        std::cout << " evectable " << it.first[0];
        std::cout << " visit count " << it.first[1] << " " << visit_count_[it.second];
        std::cout << " last visit " << it.first[2] << std::endl;
    }
}


bool LRUKReplacer::Evict(int32_t *frame_id) {
    const std::lock_guard<std::mutex> lock(latch_);
    if (cur_size_ == 0U) {
        return false;
    }
    // std::cout << "evicting " << std::endl;
    // Show();
    if (frames_.begin()->first[0] == NON_EVICTABLE) {
        return false;
    }
    auto it = frames_.begin();
    *frame_id = it->second;
    lookup_.erase(it->second);
    visit_count_.erase(it->second);
    frames_.erase(it->first);
    cur_size_--;
    return true;
}

void LRUKReplacer::RecordAccess(uint32_t frame_id) {
    const std::lock_guard<std::mutex> lock(latch_);
    //    std::cout << "Record access: " << frame_id << std::endl;
    if (frame_id < 0 || frame_id >= static_cast<uint32_t>(replacer_size_)) {
        assert(0 && "invalid frame_id");
        return;
    }
    if (lookup_.count(frame_id) != 0U) {
        std::array<size_t, 3> evic_cnt_time = lookup_[frame_id];

        frames_.erase(evic_cnt_time);

        visit_count_[frame_id]++;
        if (visit_count_[frame_id] >= k_) {
            lookup_[frame_id][1] = 1;
            lookup_[frame_id][2] = current_timestamp_;
        }
        frames_.insert({lookup_[frame_id], frame_id});
    } else {
        cur_size_++;
        std::array<size_t, 3> evic_cnt_time = {EVICTABLE, 0, current_timestamp_};
        visit_count_[frame_id] = 1;
        lookup_[frame_id] = evic_cnt_time;
        if (visit_count_[frame_id] >= k_) {
            lookup_[frame_id][1] = 1;
        }
        frames_.insert({lookup_[frame_id], frame_id});
    }
    current_timestamp_++;
}

void LRUKReplacer::SetEvictable(uint32_t frame_id, bool set_evictable) {
    const std::lock_guard<std::mutex> lock(latch_);
    // std::cout << "set evictable: " << frame_id << " " << set_evictable << std::endl;
    if (!lookup_.count(frame_id)) {
        std::cout << frame_id << "\n";
        assert(0 && "invalid frame_id");
        return;
    }
    set_evictable = !set_evictable;

    if (lookup_[frame_id][0] != static_cast<size_t>(set_evictable)) {
        frames_.erase(lookup_[frame_id]);
        lookup_[frame_id][0] = static_cast<size_t>(set_evictable);
        frames_.insert({lookup_[frame_id], frame_id});
        set_evictable = !set_evictable;
        cur_size_ += (set_evictable ? 1 : -1);
    }
}

void LRUKReplacer::Remove(uint32_t frame_id) {
    const std::lock_guard<std::mutex> lock(latch_);
    // std::cout << "Remove : " << frame_id << std::endl;
    if (lookup_.count(frame_id) == 0U) {
        return;
    }
    if (lookup_[frame_id][0] == 1) {
        assert(1 && "invalid operation");
        return;
    }

    frames_.erase(lookup_[frame_id]);
    visit_count_.erase(frame_id);
    lookup_.erase(frame_id);
    cur_size_--;
}
