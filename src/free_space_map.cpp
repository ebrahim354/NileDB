#pragma once
#include "cache_manager.cpp"
#include "page.cpp"
#include <math.h>
#include <cstring>

#define MAX_FRACTION 2 // can't be higher than page size.



// first 4 bytes in the first page are the number of pages of the table let's call it ( n ).
// next n bytes are the fractions per n pages of the table.
// the free space pages do not shrink by convention ( I should probably make a FreeSpaceMapPage class ).

class FreeSpaceMap {
    public:
        FreeSpaceMap(CacheManager* cm, PageID first_page_id): cm_(cm), first_page_id_(first_page_id){
            Page* page = cm_->fetchPage(first_page_id_);
            if(!page)
                return;
            char* d = page->data_;
            size_ = *reinterpret_cast<uint32_t*>(page->data_);
            array_ = new uint8_t[size_]; 
            int i = 4;
            int tot = 0;
            PageID page_id_ptr = first_page_id_;
            while(i < PAGE_SIZE && tot < size_){
                array_[tot] = *reinterpret_cast<uint8_t*>(page->data_+i);
                i++;
                tot++;
                if(i == PAGE_SIZE){
                    cm_->unpinPage(page_id_ptr, false);
                    page_id_ptr.page_num_++;
                    page = cm_->fetchPage(page_id_ptr);
                    i = 0;
                }
            }
            if(i != 0) 
                cm_->unpinPage(page_id_ptr, false);
        }
        ~FreeSpaceMap(){
            delete[] array_;
        }


        int addPage(uint8_t fraction){
            Page* last_page = nullptr;
            Page* first_page = cm_->fetchPage(first_page_id_);
            if(!first_page) first_page = cm_->newPage(first_page_id_.fid_);
            //if(!first_page) first_page = cm_->newPage(first_page_id_.file_name_);
            // something is wrong with the file name or just can't allocate a first fsm page, just return an error.
            if(!first_page) return 1;

            //if((size_+4) % PAGE_SIZE == 0) last_page = cm_->newPage(first_page_id_.file_name_);
            if((size_+4) % PAGE_SIZE == 0) last_page = cm_->newPage(first_page_id_.fid_);
            else last_page = getPageAtOffset(size_);

            if(!last_page) return 1;

            uint32_t slot = (size_+4)%PAGE_SIZE;
            size_++;
            memcpy(last_page->data_+slot, &fraction, sizeof(fraction));
            memcpy(first_page->data_, &size_, sizeof(size_));
            
            // this is so slow but only happens once per new fsm page so it's fine.
            uint8_t* old_array = array_;
            array_ = new uint8_t[size_]; 
            memcpy(array_, old_array, (size_-1) * sizeof(uint8_t));
            array_[size_-1] = fraction;
            delete [] old_array;
            cm_->flushPage(last_page->page_id_);
            cm_->flushPage(first_page->page_id_);
            cm_->unpinPage(last_page->page_id_, true);
            cm_->unpinPage(first_page->page_id_, true);
            return 0;
        }

        Page* getPageAtOffset(uint32_t offset){
            int page_num = ceil(double(offset+4)/PAGE_SIZE);
            PageID pid = first_page_id_;
            pid.page_num_ = page_num;
            return cm_->fetchPage(pid);
        }
        // return 1 on error.
        // offset is the table data page number.
        int updateFreeSpace(uint32_t offset, uint32_t free_space){
            // table pages are 1 indexed, and we are using  zero indexes :(.
            offset--;
            if(offset >= size_) return 1;
            uint8_t fraction = free_space / (PAGE_SIZE / MAX_FRACTION);
            array_[offset] = fraction;
            Page* page = getPageAtOffset(offset);
            uint32_t slot = (offset+4)%PAGE_SIZE;
            memcpy(page->data_+slot, &fraction, sizeof(fraction));
            cm_->flushPage(page->page_id_);
            cm_->unpinPage(page->page_id_, true);
            return 0;
        }
        // page_num (output).
        // return 1 in case of an error.
        int getFreePageNum(uint32_t freespace_needed, uint32_t* page_num){
            uint8_t fraction = freespace_needed / (PAGE_SIZE / MAX_FRACTION);
            fraction += (freespace_needed % (PAGE_SIZE / MAX_FRACTION));
            for(uint32_t i = 0; i < size_; ++i){
                 // if the fraction of a page is 0 that means it's a freelist or full page skip it.
                if(array_[i] >= fraction) {
                    // pages are number starting with 1 not 0.
                    *page_num = i+1;
                    return 0;
                }
            }
            //  didn't find enough free space.
            return 1;
        }
        
    private:
        uint8_t* array_ = nullptr;
        uint32_t size_ = 0;
        CacheManager* cm_ = nullptr;
        PageID first_page_id_ = INVALID_PAGE_ID;
};

