#ifndef FREE_SPACE_MAP_H
#define FREE_SPACE_MAP_H

#include "cache_manager.h"
#include "page.h"
#include <math.h>
#include <cstring>

#define MAX_FRACTION 256



// each i-th byte is the fractions of the i-th page of the table,
// low fraction means higher free space for the i-th page.
// tables do not delete pages unless they get compacted by a higher component of the system.
// the free space pages do not shrink, but can be replaced with new ones,
// for example when compacting the data of the table and the table is shirnking,
// then the free_space_map is cleared and the system should start a new free space map.

class FreeSpaceMap {
    public:
        void init(CacheManager* cm, FileID fid);
        void destroy();

        // return 1 on error.
        int updateFreeSpace(PageID table_pid, u32 used_space);

        // page_num (output).
        // return 1 on failure.
        int getFreePageNum(u32 freespace_needed, PageNum* out_page_num);
        
    private:
        CacheManager* cm_ = nullptr;
        FileID fid_;
};

#endif // FREE_SPACE_MAP_H
