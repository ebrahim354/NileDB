#ifndef OVERFLOW_PAGE_ITERATOR_H
#define OVERFLOW_PAGE_ITERATOR_H

#include "overflow_page.h"


// iterator for overflow pages.
// this iterator does not hold pages pinned, 
// it just: opens the page -> copy data -> advances the curser -> closes the page.
class OverflowIterator {
    public:
        OverflowIterator(CacheManager *cm, PageID page_id);
        OverflowIterator();

        const char* get_data_cpy_and_advance(Arena* arena, u16* size);
    private:
        PageID cur_page_id_ = INVALID_PAGE_ID;
        CacheManager *cache_manager_ = nullptr;
};

OverflowIterator::OverflowIterator(){}

OverflowIterator::OverflowIterator(CacheManager *cm, PageID page_id):
    cache_manager_(cm), cur_page_id_(page_id)
{
    assert(cache_manager_ != nullptr && cur_page_id_ != INVALID_PAGE_ID);
}

// the user should re-align the arena after exhausting the iterator.
const char* OverflowIterator::get_data_cpy_and_advance(Arena* arena, u16* size) {
    if(cur_page_id_ == INVALID_PAGE_ID || cur_page_id_.page_num_ == 0) return nullptr;
    auto cur_page = (OverflowPage*)cache_manager_->fetchPage(cur_page_id_);
    assert(cur_page);
    *size = cur_page->getContentSize();
    const char* output = (char*)arena->alloc(*size, 0);
    memcpy((void*)output, cur_page->getContentPtr(), *size);
    cur_page_id_.page_num_ = cur_page->getNextPageNumber();
    assert(cache_manager_->unpinPage(cur_page->page_id_, false));
    return output;
}

#endif // OVERFLOW_PAGE_ITERATOR_H
