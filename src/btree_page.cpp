#pragma once
#include "page.cpp"
#include "value.cpp"


enum class BTreePageType { INVALID_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

// this structure is used to group multiple columns into one key to support multi column indexes for example: 
// create index tmp_index on tmp_table(a,b,c);
struct IndexKey {
    std::vector<Value> keys_;

    IndexKey& operator=(const IndexKey& rhs){
        keys_.clear();
        keys_.resize(rhs.keys_.size());
        for(int i = 0; i < rhs.keys_.size(); ++i){
            keys_[i] = rhs.keys_[i]; 
        }
        return *this;
    } 
    bool operator==(const IndexKey &rhs) const { 
        if(keys_.size() != rhs.keys_.size()) return false;
        for(int i = 0; i < keys_.size(); ++i)
            if(keys_[i].type_ != rhs.keys_[i].type_ || keys_[i] != rhs.keys_[i]) return false;
        return true;
    }

    bool operator!=(const IndexKey &rhs) const {
        if(keys_.size() != rhs.keys_.size()) return true;
        for(int i = 0; i < keys_.size(); ++i)
            if(keys_[i].type_ != rhs.keys_[i].type_ || keys_[i] != rhs.keys_[i]) return true;
        return false;
    }

    // TODO: review this.
    bool operator<(const IndexKey &rhs) const {
        int sz = std::min(keys_.size(), rhs.keys_.size());
        for(int i = 0; i < sz; ++i)
            if( keys_[i] < rhs.keys_[i]) return true;
        return false;
    }

    // TODO: review this.
    bool operator<=(const IndexKey &rhs) const {
        int sz = std::min(keys_.size(), rhs.keys_.size());
        for(int i = 0; i < sz; ++i)
            if( keys_[i] <= rhs.keys_[i]) return true;
        return false;
    }

    // TODO: review this.
    bool operator>(const IndexKey &rhs) const {
        int sz = std::min(keys_.size(), rhs.keys_.size());
        for(int i = 0; i < sz; ++i)
            if( keys_[i] > rhs.keys_[i]) return true;
        return false;
    }

    // TODO: review this.
    bool operator>=(const IndexKey &rhs) const {
        int sz = std::min(keys_.size(), rhs.keys_.size());
        for(int i = 0; i < sz; ++i)
            if( keys_[i] >= rhs.keys_[i]) return true;
        return false;
    }
};

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BTreePage {
 public:
  auto IsLeafPage() const -> bool;
  auto IsRootPage() const -> bool;
  void SetPageType(BTreePageType page_type);

  auto GetSize() const -> int;
  void SetSize(int size);
  void IncreaseSize(int amount);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  auto GetMinSize() const -> int;

  auto GetParentPageId() const -> PageID;
  void SetParentPageId(PageID parent_page_id);

  auto GetPageId() const -> PageID;
  void SetPageId(PageID page_id);

 private:
  BTreePageType page_type_;
  int size_;
  int max_size_;
  PageID parent_page_id_;
  PageID page_id_;
};

auto BTreePage::IsLeafPage() const -> bool { return page_type_ == BTreePageType::LEAF_PAGE; }
auto BTreePage::IsRootPage() const -> bool { return parent_page_id_ == INVALID_PAGE_ID; }
void BTreePage::SetPageType(BTreePageType page_type) { page_type_ = page_type; }

auto BTreePage::GetSize() const -> int { return size_; }
void BTreePage::SetSize(int size) { size_ = size; }
void BTreePage::IncreaseSize(int amount) { size_ += amount; }

auto BTreePage::GetMaxSize() const -> int { return max_size_; }
void BTreePage::SetMaxSize(int size) { max_size_ = size; }

auto BTreePage::GetMinSize() const -> int { return max_size_ / 2; }

auto BTreePage::GetParentPageId() const -> PageID { return parent_page_id_; }
void BTreePage::SetParentPageId(PageID parent_page_id) { parent_page_id_ = parent_page_id; }

auto BTreePage::GetPageId() const -> PageID { return page_id_; }
void BTreePage::SetPageId(PageID page_id) { page_id_ = page_id; }

