#pragma once

#include <array>
#include <cstdint>
#include <iostream>
#include <map>
#include <mutex> 
#include <unordered_map>
#include <utility>
#include <assert.h>

#define EVICTABLE 0
#define NON_EVICTABLE 1
#define LRUK_REPLACER_K = 10;  


class LRUKReplacer {
    public:
        explicit LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
            // std::cout << "replacer_size_: " << replacer_size_ << " k: " << k << std::endl;
        }
        ~LRUKReplacer() = default;
        bool Evict(uint32_t *frame_id);
        void RecordAccess(uint32_t frame_id);
        void SetEvictable(uint32_t frame_id, bool set_evictable);
        void Remove(uint32_t frame_id);

        auto Size() {
            const std::lock_guard<std::mutex> lock(latch_);
            // std::cout << "Size" << std::endl;
            return cur_size_;
        }
        // for debuging.
        void Show() {
            for (auto it:frames_) {
                std::cout << "frame id: " << it.second;
                std::cout << " evectable " << it.first[0];
                std::cout << " visit count " << it.first[1] << " " << visit_count_[it.second];
                std::cout << " last visit " << it.first[2] << std::endl;
            }
        }


    private:
        size_t cur_size_{0};
        size_t replacer_size_;
        size_t k_;
        std::mutex latch_;
        // assume that it can grow to infinity for now but it's not true. max = 2^32 - 1(not sure 64 or 32).
        size_t current_timestamp_{0};
        // we are using two maps pointing at each other because we need a lookup_ with the frame_id (lookup_),
        // and we need frames to be sorted naturally inside the frames_ map and the frames_.begin() is the next
        // frame to be evicted. We can get rid of the visit_count_ table later but it's fine for now ( for example
        // use array<4> instead and store the length and it's not going to affect the sorting any way because
        // array[3] is unique to every frame, we can't access 2 frames at the same time stamp).
        //
        // that sorting is done by the array<3> key naturally according to the following:
        // structure of an entry inside frames_ or lookup_
        // array of length 3: arr[0] => 0 means evictable, 1 means not evictable (for free sorting from the map container)
        // we need evictables at the start of the map to just use frames_.begin() on eviction.
        //
        // arr[1] => visit count : 0 means less than k , 1 means more than or equeal to k ( less than k should be 
        // evicted first and the number of visits doesn't matter it's either you're k or not, and in case of 
        // two frames less than k they both are 0 so they get sorted according to next entry in the array).
        //
        // arr[2] => last visit time stamp. smaller = older = should be evicted first free storing again.
        // visit_count handles number of visited (can't put it on the map because we need to sort all visits that
        // are less than k and take smallest one).
        std::unordered_map<size_t, size_t> visit_count_;
        std::map<size_t, std::array<size_t, 3>> lookup_;
        std::map<std::array<size_t, 3>, size_t> frames_;
};

bool LRUKReplacer:: Evict(uint32_t *frame_id) {
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
    if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
        assert(1 && "invalid frame_id");
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
    if (lookup_.count(frame_id) == 0U) {
        assert(1 && "invalid frame_id");
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
