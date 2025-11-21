#pragma once

#include <string>
#include <shared_mutex>
#include <cstring>
#include <page.h>

bool PageID::isValidPage(){
    return (fid_ != INVALID_FID && page_num_ != INVALID_PAGE_NUM);
}

PageID& PageID::operator=(const PageID &rhs){
    this->fid_ = rhs.fid_; 
    this->page_num_ = rhs.page_num_;
    return *this;
}

bool PageID::operator<(const PageID &other) const {
    int files_match = (fid_ == other.fid_); 
    if(files_match)  return page_num_ < other.page_num_;
    return (fid_ < other.fid_);
}

bool PageID::operator==(const PageID &other) const {
    int files_match = (fid_ == other.fid_); 
    if(files_match)  return page_num_ == other.page_num_;
    return (files_match);
}

bool PageID::operator!=(const PageID &other) const {
    int files_match = (fid_ == other.fid_); 
    if(files_match)  return page_num_ != other.page_num_;
    return 1;
}

void Page::ResetMemory() {
    memset(data_, 0, PAGE_SIZE); 
}


