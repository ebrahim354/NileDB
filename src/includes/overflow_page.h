#ifndef OVERFLOW_PAGE_H
#define OVERFLOW_PAGE_H

#include "page.h"

#define OVERFLOW_PAGE_HEADER_SIZE  6
#define MAX_OVERFLOW_PAGE_CONTENT_SIZE (PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE)

class OverflowPage : public Page {
    public:
        // assumes that the ResetMemory function is called before this by the cache manager.
        void  init();
        void  setNextPageNumber(u32 next_page_number);
        u32   getNextPageNumber();  // 0 in case of last page.

        char* getContentPtr();

        void  setContentSize(u16 size);
        u16   getContentSize();
    private:
        char*    getPtrTo(u32 offset);
        static const u32 NEXT_PAGE_NUMBER_OFFSET_ = 0;
        static const u16 CONTENT_SIZE_OFFSET_ = 4;
        static const u32 CONTENT_OFFSET_ = 6;
};


void  OverflowPage::setNextPageNumber(u32 next_page_number) {
    memcpy(getPtrTo(NEXT_PAGE_NUMBER_OFFSET_), &next_page_number, sizeof(next_page_number)); 
}

u32   OverflowPage::getNextPageNumber() {
    return *(u32*)getPtrTo(NEXT_PAGE_NUMBER_OFFSET_);
}

char* OverflowPage::getPtrTo(u32 offset) {
    return data_+offset;
}

char* OverflowPage::getContentPtr() {
    return data_+CONTENT_OFFSET_;
}

void  OverflowPage::setContentSize(u16 size) {
    memcpy(getPtrTo(CONTENT_SIZE_OFFSET_), &size, sizeof(size)); 
}

u16   OverflowPage::getContentSize() {
    return *(u16*)getPtrTo(CONTENT_SIZE_OFFSET_);
}


#endif // OVERFLOW_PAGE_H
