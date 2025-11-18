#ifndef FREE_SPACE_MAP_H
#define FREE_SPACE_MAP_H

#include "cache_manager.h"
#include "page.h"
#include <math.h>
#include <cstring>

#define MAX_FRACTION 2 // can't be higher than page size.



// first 4 bytes in the first page are the number of pages of the table let's call it ( n ).
// next n bytes are the fractions per n pages of the table.
// the free space pages do not shrink by convention ( I should probably make a FreeSpaceMapPage class ).

class FreeSpaceMap {
    public:
        FreeSpaceMap(CacheManager* cm, PageID first_page_id): cm_(cm), first_page_id_(first_page_id);
        ~FreeSpaceMap();

        int addPage(uint8_t fraction);
        Page* getPageAtOffset(uint32_t offset);

        // return 1 on error.
        // offset is the table data page number.
        int updateFreeSpace(uint32_t offset, uint32_t free_space);

        // page_num (output).
        // return 1 in case of an error.
        int getFreePageNum(uint32_t freespace_needed, uint32_t* page_num);
        
    private:
        uint8_t* array_ = nullptr;
        uint32_t size_ = 0;
        CacheManager* cm_ = nullptr;
        PageID first_page_id_ = INVALID_PAGE_ID;
};

#endif // FREE_SPACE_MAP_H
