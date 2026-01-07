#ifndef LRU_K_REPLACER_H
#define LRU_K_REPLACER_H



#define EVICTABLE 0
#define NON_EVICTABLE 1
#define LRUK_REPLACER_K = 10;  


class LRUKReplacer {
    public:
        explicit LRUKReplacer(size_t num_frames, size_t k);
        ~LRUKReplacer() = default;
        bool Evict(int32_t *frame_id);
        void RecordAccess(uint32_t frame_id);
        void SetEvictable(uint32_t frame_id, bool set_evictable);
        void Remove(uint32_t frame_id);

        auto Size();
        // for debuging.
        void Show();


    private:
        size_t cur_size_{0};
        size_t replacer_size_;
        size_t k_;
        std::mutex latch_;
        // assume that it can grow to infinity for now but it's not true. max = 2^64 - 1
        u64 current_timestamp_{0};
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
        std::unordered_map<size_t, std::array<size_t, 3>> lookup_;
        std::map<std::array<size_t, 3>, size_t> frames_;
};


#endif // LRU_K_REPLACER_H
