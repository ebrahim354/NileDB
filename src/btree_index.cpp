#pragma once
#include <shared_mutex>
#include <deque>
#include "cache_manager.cpp"
#include "page.cpp"
#include "record.cpp"
#include "index_iterator.cpp"
#include "free_space_map.cpp"
#include "btree_page.cpp"
#include "btree_leaf_page.cpp"
#include "btree_internal_page.cpp"
#include "table_schema.cpp"

class BTreeIndex {
    public:
        BTreeIndex(CacheManager* cm, 
                FileID fid, 
                PageID root_page_id,
                TableSchema* index_meta_schema
                ):
            cache_manager_(cm),
            fid_(fid),
            root_page_id_(root_page_id),
            index_meta_schema_(index_meta_schema)
            {}
        ~BTreeIndex(){}

        void update_index_root(FileID fid, PageNum new_root_pid) {
          //TableSchema* indexes_meta_schema = tables_["NDB_INDEX_META"];
          TableSchema* indexes_meta_schema = index_meta_schema_; 
          TableIterator* it_meta = indexes_meta_schema->getTable()->begin();

          while(it_meta->advance()){
            Record r = it_meta->getCurRecordCpy();
            std::vector<Value> values;
            int err = indexes_meta_schema->translateToValues(r, values);
            assert(err == 0 && "Could not traverse the indexes schema.");

            std::string index_name = values[0].getStringVal();
            std::string table_name = values[1].getStringVal();
            FileID index_fid = values[2].getIntVal();
            PageNum root_pid = values[3].getIntVal();
            if(index_fid != fid) continue;

            RecordID rid = it_meta->getCurRecordID();

            values[3] = Value(new_root_pid);
            Record* new_record = indexes_meta_schema->translateToRecord(values);
            err = indexes_meta_schema->getTable()->updateRecord(&rid, *new_record);
            assert(err == 0 && "Could not update record.");
            delete new_record;
          }
          delete it_meta;
        }

        void SetRootPageId(PageID root_page_id, int insert_record = 0) {
            // TODO: fix dead lock.
            //std::unique_lock locker(this->root_page_id_lock_);
            root_page_id_ = root_page_id;
            assert(index_meta_schema_ != 0);
            update_index_root(fid_, root_page_id.page_num_);
        }

        // true means value is returned.
        bool GetValue(IndexKey &key, std::vector<RecordID> *result) {
            // read lock on the root_page_id_
            // std::cerr << "GET VALUE CALL\n";
            // TODO: fix dead lock.
            //std::shared_lock locker(root_page_id_lock_);
            // std::cerr << "HI searching for: " << key << std::endl;
            if (root_page_id_ == INVALID_PAGE_ID) {
                return false;
            }
            auto *start_page = cache_manager_->fetchPage(root_page_id_);
            // could not fetch page.
            if(!start_page) return false;

            // TODO: fix dead lock.
            // start_page->mutex_.lock_shared();
            auto *start = reinterpret_cast<BTreePage *>(start_page->data_);
            while (!start->IsLeafPage()) {
                auto *cur = reinterpret_cast<BTreeInternalPage *>(start);
                PageID page_id = cur->NextPage(key, fid_);
                auto *tmp = start_page;
                auto tmp_page_id = start->GetPageId(fid_);

                start_page = cache_manager_->fetchPage(page_id);
                // could not fetch page.
                if(!start_page) return false;

                // TODO: fix dead lock.
                // start_page->mutex_.lock_shared();
                start = reinterpret_cast<BTreePage *>(start_page->data_);

                // TODO: fix dead lock.
                // tmp->mutex_.unlock_shared();
                cache_manager_->unpinPage(tmp_page_id, false);
                if (tmp_page_id == root_page_id_) {
                    // TODO: fix dead lock.
                    // locker.unlock();
                }
            }
            auto *leaf = reinterpret_cast<BTreeLeafPage *>(start);
            auto status = leaf->GetValue(key, result);

            // TODO: fix dead lock.
            // start_page->mutex_.unlock_shared();
            cache_manager_->unpinPage(start->GetPageId(fid_), false);
            return status;
        }


        // new_page_raw (output).
        BTreeLeafPage* create_leaf_page(PageID parent_pid, Page** new_page_raw){
          *new_page_raw = cache_manager_->newPage(fid_);
          if(!(*new_page_raw)) return nullptr;
          auto new_page_id = (*new_page_raw)->page_id_;
          // write latch.
          // TODO: fix dead lock.
          // new_page_raw->mutex_.lock();
          auto *new_page = reinterpret_cast<BTreeLeafPage *>((*new_page_raw)->data_);
          new_page->Init(new_page_id, parent_pid);
          new_page->SetPageType(BTreePageType::LEAF_PAGE);
          return new_page;
        }

        // new_page_raw (output).
        BTreeInternalPage* create_internal_page(PageID parent_pid, Page** new_page_raw){
          *new_page_raw = cache_manager_->newPage(fid_);
          if(!(*new_page_raw)) 
            return nullptr;
          auto new_page_id = (*new_page_raw)->page_id_;
          //TODO: Fix DEAD LOCK.
          //new_page_raw->mutex_.lock();
          auto *new_page = reinterpret_cast<BTreeInternalPage *>((*new_page_raw)->data_);
          new_page->Init(new_page_id, parent_pid);
          new_page->SetPageType(BTreePageType::INTERNAL_PAGE);
          return new_page;
        }


        // return true if inserted successfully.
        bool Insert(const IndexKey &key, const RecordID &value) {
            std::unique_lock locker(root_page_id_lock_);
            if((key.size_ + 16) * 3 > BTreePage::get_max_key_size()){
              std::cout << "Key can't fit in one page\n";
              return false;
            }
            if(!fid_to_fname.count(fid_))  return false;
            std::deque<Page *> page_deque;
            BTreePage *root;
            if (root_page_id_ == INVALID_PAGE_ID) {
                // if no root then the new root will be treated as a leaf node.
                auto *leaf_page = cache_manager_->newPage(fid_);
                // could not create a page.
                if(!leaf_page) 
                    return false;
                // write latch.
                // TODO: FIX dead lock
                // leaf_page->mutex_.lock();
                page_deque.push_back(leaf_page);
                auto *leaf = reinterpret_cast<BTreeLeafPage *>(leaf_page->data_);

                leaf->Init(leaf_page->page_id_, INVALID_PAGE_ID);
                leaf->SetPageType(BTreePageType::LEAF_PAGE);
                root = leaf;
                SetRootPageId(leaf_page->page_id_, 1);
            } else {
                auto *root_page = cache_manager_->fetchPage(root_page_id_);
                // could not fetch a page.
                if(!root_page) 
                    return false;
                // write latch.
                // TODO: FIX dead lock
                //root_page->mutex_.lock();
                page_deque.push_back(root_page);
                root = reinterpret_cast<BTreePage *>(root_page->data_);
            }
            // traverse the tree untill you find the leaf node
            // and keep track of the closest empty pointer on a stack in case of a cascading split.
            std::deque<BTreePage *> custom_stk;
            custom_stk.push_back(root);
            while (!custom_stk.back()->IsLeafPage()) {
                auto *tmp = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                auto page_id = tmp->NextPage(key, fid_);

                auto *ptr_page = cache_manager_->fetchPage(page_id);
                // could not fetch page.
                if(!ptr_page) 
                    return false;
                // write latch.
                // TODO: FIX DEAD LOCK.
                //ptr_page->mutex_.lock();
                auto *ptr = reinterpret_cast<BTreePage *>(ptr_page->data_);

                bool is_full;
                if (ptr->IsLeafPage()) {
                    auto *place_holder = reinterpret_cast<BTreeLeafPage *>(ptr);
                    is_full = place_holder->IsFull(key);
                } else {
                    auto *place_holder = reinterpret_cast<BTreeInternalPage *>(ptr);
                    is_full = place_holder->IsFull(key);
                }

                // meaning the the current node is empty weather it's an internal or a leaf node.
                // clear the stack and unpin everything (will be edited on the concurrency part).
                // and ulock every thing (use a dequeue to remove from the front not the top).
                if (!is_full) {
                    while (!custom_stk.empty()) {
                        auto *cur = custom_stk.front();
                        auto *cur_page = page_deque.front();
                        custom_stk.pop_front();
                        page_deque.pop_front();
                        // write unlatch.
                        cur_page->mutex_.unlock();
                        if (cur->GetPageId(fid_) == root_page_id_) {
                            locker.unlock();
                        }
                        cache_manager_->unpinPage(cur->GetPageId(fid_), false);
                    }
                }
                page_deque.push_back(ptr_page);
                custom_stk.push_back(ptr);
            }
            // now we have a stack of the nodes that we would need in the worst case which is splitting all over to the top
            // and the leaf node is at the top of the stack.

            // start by getting the top node best case: it's not full so just insert the "current key, value pair"
            // in case of a split create a new node of the same type split the keys between them
            // add the current key, value to one of them
            // and re assign the key to the middle key and the value to the page_id of the new node
            // and so on untill the stack is empty.
            // one edge case is when the top node is actually the root node and it's full too so we create a new node
            // and update the root id.
            auto current_key = key;
            auto current_value = value;
            auto current_internal_value = INVALID_PAGE_ID;
            bool inserted = false;
            while (!custom_stk.empty()) {
                if (custom_stk.back()->IsLeafPage()) {
                    auto *cur = reinterpret_cast<BTreeLeafPage *>(custom_stk.back());
                    // if it's empty then add the key
                    bool full = cur->IsFull(key);
                    if (!full) {
                        inserted = cur->Insert(current_key, current_value);
                        break;
                    }
                    // in case of non root splits:
                    Page* new_page_raw = nullptr;
                    auto new_page = create_leaf_page(cur->GetParentPageId(fid_), &new_page_raw);
                    if(!new_page_raw) return false;
                    auto new_page_id = new_page_raw->page_id_;
                    // write latch.
                    // TODO: fix dead lock.
                    //new_page_raw->mutex_.lock();

                    inserted = cur->split_with_and_insert(new_page, current_key, current_value);
                    assert(inserted == true);
                    IndexKey middle_key = cur->get_last_key_cpy(); // TODO: fix leak.
                    current_key = middle_key;
                    current_internal_value = new_page_id;

                    // the edge case of the root being the current and the root is not empty
                    // we add two nodes inestead of just one.
                    // the splitted node and the new root.
                    if (cur->IsRootPage()) {
                      // create a new root
                      Page* new_root_raw = nullptr;
                      auto new_root = create_internal_page(INVALID_PAGE_ID, &new_root_raw);
                      if(!new_root_raw) return false;
                      auto tmp_root_id = new_root_raw->page_id_;
                      // create a new root

                      // every root starts with only 1 key and 2 pointers.
                      // TODO: FIX STARTING KEY.
                      new_root->set_num_of_slots(2);
                      new_root->SetValAt(0, cur->GetPageId(fid_));
                      new_root->SetKeyAt(1, current_key);
                      new_root->SetValAt(1, new_page_id);
                      SetRootPageId(tmp_root_id);
                      new_root_raw->mutex_.unlock();
                      cache_manager_->unpinPage(root_page_id_, true);

                      cur->SetParentPageId(root_page_id_);
                      new_page->SetParentPageId(root_page_id_);
                    }
                    new_page->set_next_page_number(cur->get_next_page_number());
                    cur->set_next_page_number(new_page_id.page_num_);

                    // unpin the current_page and the new_page we don't need them any more and they are dirty.
                    page_deque.back()->mutex_.unlock();
                    new_page_raw->mutex_.unlock();
                    cache_manager_->unpinPage(cur->GetPageId(fid_), true);
                    cache_manager_->unpinPage(new_page->GetPageId(fid_), true);

                    custom_stk.pop_back();
                    page_deque.pop_back();
                } else {
                    auto *cur = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                    // if it's empty then add the key
                    bool full = cur->IsFull(key);
                    if (!full) {
                        cur->Insert(current_key, current_internal_value);
                        break;
                    }

                    // in case of non root splits:
                    auto *new_page_raw = cache_manager_->newPage(fid_);
                    // could not create page.
                    if(!new_page_raw) 
                        return false;
                    auto new_page_id = new_page_raw->page_id_;
                    // TODO: Fix dead lock.
                    //new_page_raw->mutex_.lock();
                    auto *new_page = reinterpret_cast<BTreeInternalPage *>(new_page_raw->data_);
                    new_page->Init(new_page_id, cur->GetParentPageId(fid_));
                    new_page->SetPageType(BTreePageType::INTERNAL_PAGE);
                    //int md = std::ceil(static_cast<float>(cur->GetMaxSize() + 1) / 2);
                    //TODO: FIX md value.
                    int md = std::ceil(static_cast<float>(cur->get_num_of_slots() + 1) / 2);
                    int pos = cur->InsertionPosition(current_key);
                    int inserted_on_new = 0;  // 1 => on new page, 0 => no insertion, -1 => cur page.
                    if (pos > md) {
                        new_page->SetValAt(0, cur->ValueAt(md, fid_));
                        inserted_on_new = 1;
                    } else if (pos < md) {
                        md--;
                        new_page->SetValAt(0, cur->ValueAt(md, fid_));
                        inserted_on_new = -1;
                    } else {
                        new_page->SetValAt(0, current_internal_value);
                    }
                    auto *tmp1_page = cache_manager_->fetchPage(new_page->ValueAt(0, fid_));
                    // could not fetch page.
                    if(!tmp1_page) 
                        return false;
                    //TODO: Fix dead lock.
                    //tmp1_page->mutex_.lock();
                    auto *tmp1 = reinterpret_cast<BTreeInternalPage *>(tmp1_page->data_);
                    tmp1->SetParentPageId(new_page_id);
                    tmp1_page->mutex_.unlock();
                    cache_manager_->unpinPage(tmp1->GetPageId(fid_), true);
                    // cur key = 3, cur val = 4.
                    // pos > md => md = 2
                    // pos < md => md = 1
                    // pos == md => md = 2 but not need to skip a the mid we already are skipping it.
                    int inc = 1;
                    if (inserted_on_new == 0) {
                        inc = 0;
                    }
                    for (int i = md + inc, j = 1; i < cur->get_num_of_slots(); i++, j++) {
                        new_page->increase_size(1);
                        new_page->SetKeyAt(j, cur->KeyAt(i));
                        new_page->SetValAt(j, cur->ValueAt(i, fid_));

                        auto *tmp_page = cache_manager_->fetchPage(new_page->ValueAt(j, fid_));
                        // could not fetch page.
                        if(!tmp_page)
                            return false;
                        // TODO: Fix daed lock.
                        // tmp_page->mutex_.lock();
                        auto *tmp = reinterpret_cast<BTreeInternalPage *>(tmp_page->data_);
                        tmp->SetParentPageId(new_page_id);
                        tmp_page->mutex_.unlock();
                        cache_manager_->unpinPage(tmp->GetPageId(fid_), true);
                    }
                    IndexKey middle_key = current_key;
                    if (inserted_on_new != 0) {
                        middle_key = cur->KeyAtCpy(md);
                    }
                    cur->set_num_of_slots(md);
                    if (inserted_on_new == -1) {
                        cur->Insert(current_key, current_internal_value);

                        auto *tmp2_page = cache_manager_->fetchPage(current_internal_value);
                        // could not fetch page.
                        if(!tmp2_page)
                            return false;
                        // TODO: Fix Dead lock
                        //tmp2_page->mutex_.lock();
                        auto *tmp2 = reinterpret_cast<BTreeInternalPage *>(tmp2_page->data_);

                        tmp2->SetParentPageId(cur->GetPageId(fid_));

                        // TODO: Fix Dead lock.
                        //tmp2_page->mutex_.lock();
                        cache_manager_->unpinPage(tmp2->GetPageId(fid_), true);
                    } else if (inserted_on_new == 1) {
                        new_page->Insert(current_key, current_internal_value);

                        auto *tmp2_page = cache_manager_->fetchPage(current_internal_value);
                        // could not fetch page.
                        if(!tmp2_page)
                            return false;
                        // TODO: Fix Dead lock.
                        //tmp2_page->mutex_.lock();
                        auto *tmp2 = reinterpret_cast<BTreeInternalPage *>(tmp2_page->data_);

                        tmp2->SetParentPageId(new_page_id);

                        tmp2_page->mutex_.unlock();
                        cache_manager_->unpinPage(tmp2->GetPageId(fid_), true);
                    }

                    current_key = middle_key;
                    current_internal_value = new_page_id;
                    // the edge case of the root being the current and the root is not empty
                    // we add two nodes inestead of just one.
                    // the splitted node and the new root.
                    if (cur->IsRootPage()) {
                        // create a new root
                        auto *new_root_raw = cache_manager_->newPage(fid_);
                        // could not create page.
                        if(!new_root_raw)
                            return false;
                        auto tmp_root_id = new_root_raw->page_id_;
                        // TODO: Fix Dead lock.
                        // new_root_raw->mutex_.lock();
                        auto *new_root = reinterpret_cast<BTreeInternalPage *>(new_root_raw->data_);
                        new_root->Init(tmp_root_id, INVALID_PAGE_ID);
                        new_root->SetPageType(BTreePageType::INTERNAL_PAGE);

                        //asm("int3");
                        // every root starts with only 1 key and 2 pointers.
                        new_root->set_num_of_slots(2);
                        new_root->SetValAt(0, cur->GetPageId(fid_));
                        new_root->SetKeyAt(1, current_key);
                        new_root->SetValAt(1, current_internal_value);

                        SetRootPageId(tmp_root_id);
                        new_root_raw->mutex_.unlock();
                        cache_manager_->unpinPage(root_page_id_, true);

                        cur->SetParentPageId(root_page_id_);
                        new_page->SetParentPageId(root_page_id_);
                    }
                    // unpin the current_page and the new_page we don't need them any more and they are dirty.
                    page_deque.back()->mutex_.unlock();
                    new_page_raw->mutex_.unlock();
                    cache_manager_->unpinPage(cur->GetPageId(fid_), true);
                    cache_manager_->unpinPage(new_page->GetPageId(fid_), true);

                    custom_stk.pop_back();
                    page_deque.pop_back();
                }
            }
            // in case of the key not getting inserted because of duplication we need to clear the stack.
            while (!custom_stk.empty()) {
                page_deque.front()->mutex_.unlock();
                cache_manager_->unpinPage(custom_stk.front()->GetPageId(fid_), true);
                custom_stk.pop_front();
                page_deque.pop_front();
            }

            cache_manager_->flushAllPages(); // TODO: this is not the job of the index.
            return inserted;
        }


        void Remove(const IndexKey &key) {
            std::unique_lock locker(root_page_id_lock_);
            //  std::cerr << "removing " << key << std::endl;
            if (root_page_id_ == INVALID_PAGE_ID) {
                return;
            }
            std::deque<Page *> page_deque;
            std::deque<BTreePage *> custom_stk;
            auto lock_cnt = 0;
            auto *root_page = cache_manager_->fetchPage(root_page_id_);
            lock_cnt++;
            // TODO: Fix Dead Lock.
            //root_page->mutex_.lock();
            std::cout << "YO grapped the lock\n";
            auto *root = reinterpret_cast<BTreePage *>(root_page->data_);
            // traverse the tree untill you find the leaf node
            // and keep track of the closest pointer with more than m/2 on a stack in case of a cascading merge.
            custom_stk.push_back(root);
            page_deque.push_back(root_page);
            while (!custom_stk.back()->IsLeafPage()) {
                auto *tmp = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                auto page_id = tmp->NextPage(key, fid_);

                auto *ptr_page = cache_manager_->fetchPage(page_id);
                lock_cnt++;
                // TODO: Fix Dead Lock.
                //ptr_page->mutex_.lock();
                auto *ptr = reinterpret_cast<BTreePage *>(ptr_page->data_);

                page_deque.push_back(ptr_page);
                custom_stk.push_back(ptr);
            }

            while (!custom_stk.empty()) {
                bool cur_unpined = false;
                auto tmp_cur_id = custom_stk.back()->GetPageId(fid_);
                auto *tmp_cur_page = page_deque.back();
                if (custom_stk.back()->IsLeafPage()) {
                    auto *cur_page = page_deque.back();
                    auto *cur = reinterpret_cast<BTreeLeafPage *>(custom_stk.back());
                    custom_stk.pop_back();
                    page_deque.pop_back();
                    // check to see if the key is deleted or not.
                    auto deleted = cur->Remove(key);
                    bool too_short = cur->TooShort();
                    // no deletion happened or the node is not short even after deletion then just get out.
                    // if the current leaf is the root can't merge just return.
                    if (!too_short || !deleted || cur->IsRootPage()) {
                        auto tmp_id = cur->GetPageId(fid_);
                        lock_cnt--;
                        cur_page->mutex_.unlock();
                        cache_manager_->unpinPage(tmp_id, deleted);
                        if (tmp_id == root_page_id_) {
                            locker.unlock();
                        }
                        cur_unpined = true;
                        break;
                    }
                    // handle merges for leaf nodes
                    // (if no. entries on cur + no. entries on sibling"left or right" is less than max size of a leaf node).
                    // left and right are going to be fetched using iterators and check if they share the same parent with
                    // the current node.
                    auto next_page_id = cur->GetNextPageId(fid_);
                    //auto cur_size = cur->get_num_of_slots();
                    auto cur_size = cur->get_num_of_slots();
                    auto cur_page_id = cur->GetPageId(fid_);
                    auto parent_page_id = cur->GetParentPageId(fid_);
                    // check it out later IMPORTANT.
                    // auto *parent = reinterpret_cast<InternalPage *>(cache_manager_->fetchPage(parent_page_id)->data_);
                    auto *parent = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                    //      auto pos = parent->InsertionPosition(key, comparator_);
                    //     pos--;
                    PageID prev_page_id = INVALID_PAGE_ID;
                    auto prev_pos = parent->PrevPageOffset(key);
                    if (prev_pos != -1) {
                        prev_page_id = parent->ValueAt(prev_pos, fid_);
                    }
                    // check it out later IMPORTANT.
                    auto done = false;
                    if (prev_page_id != INVALID_PAGE_ID && !done) {
                        //std::cout << "------------------------- leaf prev merge\n";
                        auto *prev_page = cache_manager_->fetchPage(prev_page_id);
                        lock_cnt++;
                        //TODO: Fix Dead Lock.
                        //prev_page->mutex_.lock();
                        auto *prev = reinterpret_cast<BTreeLeafPage *>(prev_page->data_);
                        //auto prev_size = prev->get_num_of_slots();
                        auto prev_size = prev->get_num_of_slots();
                        //if (prev->GetParentPageId(fid_) == parent_page_id && prev_size + cur_size < cur->GetMaxSize()) {
                        if (prev->GetParentPageId(fid_) == parent_page_id && cur->can_merge_with_me(prev)) {
                            // std::cout << cur->get_num_of_slots() << " " << cur->GetNextPageId(fid_) << std::endl;
                            done = true;
                            // merge into prev and delete cur, update the parent(remove the key-value) then break.
                            for (int i = 0; i < cur_size; i++) {
                                prev->SetKeyAt(prev_size + i, cur->KeyAt(i));
                                prev->SetValAt(prev_size + i, cur->ValAt(i));
                            }
                            prev->increase_size(cur_size);
                            prev->SetNextPageId(cur->GetNextPageId(fid_));

                            lock_cnt--;
                            cur_page->mutex_.unlock();
                            cache_manager_->unpinPage(cur_page_id, true);
                            cache_manager_->deletePage(cur_page_id);
                            cur_unpined = true;

                            // clear parent
                            auto pos = prev_pos + 1;
                            for (int i = pos; i < parent->get_num_of_slots() - 1; i++) {
                                parent->SetKeyAt(i, parent->KeyAt(i + 1));
                                parent->SetValAt(i, parent->ValueAt(i + 1, fid_));
                            }
                            parent->increase_size(-1);
                        }
                        //std::cout << "------------------------- leaf prev merge everything is just fine\n";
                        //std::cout << prev->GetPageId(fid_) << " " << prev_page_id << std::endl;
                        lock_cnt--;
                        prev_page->mutex_.unlock();
                        cache_manager_->unpinPage(prev_page_id, true);
                    }
                    // next node.
                    if (next_page_id != INVALID_PAGE_ID && !done) {
                        //std::cout << "------------------------- leaf next merge\n";
                        // merge into cur and delete next then break.
                        auto *next_page = cache_manager_->fetchPage(next_page_id);
                        lock_cnt++;
                        // TODO: Fix Dead Lock.
                        // next_page->mutex_.lock();
                        auto *next = reinterpret_cast<BTreeLeafPage *>(next_page->data_);
                        auto next_size = next->get_num_of_slots();
                        bool got_in = false;
                        //if (next->GetParentPageId(fid_) == parent_page_id && next_size + cur_size < cur->GetMaxSize()) {
                        if (next->GetParentPageId(fid_) == parent_page_id && cur->can_merge_with_me(next)) {
                            //std::cout << next_page_id << " " << next->GetNextPageId(fid_) << std::endl;
                            done = true;
                            got_in = true;
                            for (int i = 0; i < next_size; i++) {
                                cur->SetKeyAt(cur_size + i, next->KeyAt(i));
                                cur->SetValAt(cur_size + i, next->ValAt(i));
                            }
                            cur->increase_size(next_size);
                            cur->SetNextPageId(next->GetNextPageId(fid_));

                            lock_cnt--;
                            next_page->mutex_.unlock();
                            cache_manager_->unpinPage(next_page_id, true);
                            cache_manager_->deletePage(next_page_id);

                            // clear parent
                            auto pos = parent->NextPageOffset(key);
                            for (int i = pos; i < parent->get_num_of_slots() - 1; i++) {
                                parent->SetKeyAt(i, parent->KeyAt(i + 1));
                                parent->SetValAt(i, parent->ValueAt(i + 1, fid_));
                            }
                            parent->increase_size(-1);
                        }
                        if (!got_in) {
                            lock_cnt--;
                            next_page->mutex_.unlock();
                            cache_manager_->unpinPage(next_page_id, false);
                        }
                    }
                    // at this point merging is not an option.
                    // handle redistributions for leaf nodes.
                    // take the last element of the left node add it to the right
                    // then update the key inside the parent to point to the last element on the left(prev).
                    if (prev_page_id != INVALID_PAGE_ID && !done) {
                        //std::cout << "------------------------- leaf prev re\n";
                        auto *prev_page = cache_manager_->fetchPage(prev_page_id);
                        lock_cnt++;
                        // TODO: Fix Dead Lock.
                        // prev_page->mutex_.lock();
                        auto *prev = reinterpret_cast<BTreeLeafPage *>(prev_page->data_);
                        auto prev_size = prev->get_num_of_slots();
                        bool got_in = false;

                        if (prev->GetParentPageId(fid_) == parent_page_id && !prev->TooShortBefore()) {
                            done = true;
                            got_in = true;
                            // take last key from prev insert it into current then update parent.
                            cur->Insert(prev->KeyAt(prev_size), prev->ValAt(prev_size));
                            prev->increase_size(-1);
                            prev_size--;
                            cur_size++;

                            auto pos = parent->PrevPageOffset(key);
                            pos++;
                            parent->SetKeyAt(pos, prev->KeyAt(prev_size - 1));
                        }
                        lock_cnt--;
                        prev_page->mutex_.unlock();
                        cache_manager_->unpinPage(prev_page_id, got_in);
                    }
                    if (next_page_id != INVALID_PAGE_ID && !done) {
                        //std::cout << "------------------------- leaf next re\n";
                        auto *next_page = cache_manager_->fetchPage(next_page_id);
                        lock_cnt++;
                        // TODO: Fix Dead Lock.
                        // next_page->mutex_.lock();
                        auto *next = reinterpret_cast<BTreeLeafPage *>(next_page->data_);
                        auto next_size = next->get_num_of_slots();
                        auto got_in = false;

                        if (next->GetParentPageId(fid_) == parent_page_id && !next->TooShortBefore()) {
                            done = true;
                            got_in = true;
                            // take first key from next insert it into current then update parent.
                            cur->Insert(next->KeyAt(0), next->ValAt(0));
                            cur_size++;

                            next->Remove(next->KeyAt(0));
                            next_size--;
                            // update_parent parent
                            auto pos = parent->NextPageOffset(key);
                            pos--;
                            parent->SetKeyAt(pos, cur->KeyAt(cur_size - 1));
                        }
                        lock_cnt--;
                        next_page->mutex_.unlock();
                        cache_manager_->unpinPage(next_page_id, got_in);
                    }
                    // unpin parent. // check it out IMPORTANT.
                    //      cache_manager_->unpinPage(parent_page_id, true);
                } else {
                    // handling merges and redistributions for internal nodes "That is too much, I need help :("
                    // main differences: need to check for root page because there are no siblings.
                    // so if there is only one sibling remaining then just update the root id to that sibling.
                    auto *cur_page = page_deque.back();
                    auto *cur = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                    page_deque.pop_back();
                    custom_stk.pop_back();
                    bool too_short = cur->TooShort();
                    if (!too_short) {
                        lock_cnt--;
                        cur_page->mutex_.unlock();
                        cache_manager_->unpinPage(cur->GetPageId(fid_), true);
                        cur_unpined = true;
                        break;
                    }
                    if (cur->IsRootPage()) {
                        SetRootPageId(cur->ValueAt(0, fid_));

                        auto *child_page = cache_manager_->fetchPage(cur->ValueAt(0, fid_));
                        lock_cnt++;
                        // TODO: Fix Dead lock.
                        // child_page->mutex_.lock();
                        auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                        child->SetParentPageId(INVALID_PAGE_ID);
                        lock_cnt--;
                        child_page->mutex_.unlock();
                        cache_manager_->unpinPage(cur->ValueAt(0, fid_), true);

                        lock_cnt--;
                        cur_page->mutex_.unlock();
                        cache_manager_->unpinPage(cur->GetPageId(fid_), true);
                        cache_manager_->deletePage(cur->GetPageId(fid_));
                        cur_unpined = true;
                        break;
                    }
                    // first merges:
                    // no prev and next pointer so need to get them from the parent.
                    // may as well just fetch the parent right away.
                    auto cur_page_id = cur->GetPageId(fid_);
                    // auto parent_page_id = cur->GetParentPageId(fid_);
                    auto cur_size = cur->get_num_of_slots();
                    // IMPORTANT.
                    auto *parent = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());

                    auto parent_size = parent->get_num_of_slots();
                    bool done = false;
                    auto prev_page_id = INVALID_PAGE_ID;
                    auto next_page_id = INVALID_PAGE_ID;
                    auto prev_pos = parent->PrevPageOffset(key);
                    auto next_pos = parent->NextPageOffset(key);

                    if (prev_pos != -1) {
                        prev_page_id = parent->ValueAt(prev_pos, fid_);
                    }
                    if (next_pos < parent_size) {
                        next_page_id = parent->ValueAt(next_pos, fid_);
                    }

                    if (prev_page_id != INVALID_PAGE_ID && !done) {
                        // DONE----------
                        //std::cout << "------------------------- prev merge\n";
                        // merge into prev then delete cur.
                        auto *prev_page = cache_manager_->fetchPage(prev_page_id);
                        lock_cnt++;
                        // TODO: Fix Dead Lock.
                        //prev_page->mutex_.lock();
                        auto *prev = reinterpret_cast<BTreeInternalPage *>(prev_page->data_);
                        auto prev_size = prev->get_num_of_slots();
                        //if (prev_size + cur_size <= prev->GetMaxSize()) {
                        if (prev->can_merge_with_me(cur)) {
                            auto pos = prev_pos + 1;
                            // add the key from the parent and the first pointer from current as a new key-value pair.
                            auto parent_key = parent->KeyAt(pos);
                            prev->SetKeyAt(prev_size, parent_key);
                            prev->SetValAt(prev_size, cur->ValueAt(0, fid_));
                            prev->increase_size(1);
                            prev_size++;

                            for (int i = 0; i < cur->get_num_of_slots(); i++) {
                                auto child_page = cache_manager_->fetchPage(cur->ValueAt(i, fid_));
                                lock_cnt++;
                                // TODO: Fix Dead Lock.
                                // child_page->mutex_.lock();
                                auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                                auto tmp_child_page_id = child->GetPageId(fid_);

                                child->SetParentPageId(prev_page_id);

                                lock_cnt--;
                                child_page->mutex_.unlock();
                                cache_manager_->unpinPage(tmp_child_page_id, true);
                            }

                            for (int i = 1; i < cur_size; i++) {
                                prev->SetKeyAt(prev_size + (i - 1), cur->KeyAt(i));
                                prev->SetValAt(prev_size + (i - 1), cur->ValueAt(i, fid_));
                            }
                            prev->increase_size((cur_size - 1));
                            lock_cnt--;
                            cur_page->mutex_.unlock();
                            cache_manager_->unpinPage(cur_page_id, true);
                            cache_manager_->deletePage(cur_page_id);
                            cur_unpined = true;
                            // clear parent
                            for (int i = pos; i < parent->get_num_of_slots() - 1; i++) {
                                parent->SetKeyAt(i, parent->KeyAt(i + 1));
                                parent->SetValAt(i, parent->ValueAt(i + 1, fid_));
                            }
                            parent->increase_size(-1);
                            done = true;
                        }
                        lock_cnt--;
                        prev_page->mutex_.unlock();
                        cache_manager_->unpinPage(prev_page_id, done);
                    }
                    // next node
                    if (next_page_id != INVALID_PAGE_ID && !done) {
                        // DONE----------
                        //std::cout << "------------------------- next merge\n";
                        // merge into cur then delete next
                        auto *next_page = cache_manager_->fetchPage(next_page_id);
                        lock_cnt++;
                        //TODO: Fix  Dead Lock.
                        //next_page->mutex_.lock();
                        auto *next = reinterpret_cast<BTreeInternalPage *>(next_page->data_);
                        auto next_size = next->get_num_of_slots();
                        auto parent_key = parent->KeyAt(next_pos);
                        //if (next_size + cur_size <= cur->GetMaxSize()) {
                        if (cur->can_merge_with_me(next)) {
                            // add the key from the parent and the first pointer from next as a new key-value pair.
                            cur->SetKeyAt(cur_size, parent_key);
                            cur->SetValAt(cur_size, next->ValueAt(0, fid_));
                            //std::cout << next->ValueAt(1) << std::endl;
                            for (int i = 0; i < next->get_num_of_slots(); i++) {
                                auto *child_page = cache_manager_->fetchPage(next->ValueAt(i, fid_));
                                lock_cnt++;
                                // TODO: Fix Dead Lock.
                                //child_page->mutex_.lock();
                                auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                                auto tmp_child_page_id = child->GetPageId(fid_);
                                child->SetParentPageId(cur_page_id);
                                lock_cnt--;
                                child_page->mutex_.unlock();
                                cache_manager_->unpinPage(tmp_child_page_id, true);
                            }
                            cur->increase_size(1);
                            cur_size++;

                            for (int i = 1; i < next_size; i++) {
                                cur->SetKeyAt(cur_size + (i - 1), next->KeyAt(i));
                                cur->SetValAt(cur_size + (i - 1), next->ValueAt(i, fid_));
                            }
                            cur->increase_size((next_size - 1));
                            cur_size += (next_size - 1);
                            lock_cnt--;
                            next_page->mutex_.unlock();
                            cache_manager_->unpinPage(next_page_id, true);
                            cache_manager_->deletePage(next_page_id);
                            // clear parent
                            for (int i = next_pos; i < parent->get_num_of_slots() - 1; i++) {
                                parent->SetKeyAt(i, parent->KeyAt(i + 1));
                                parent->SetValAt(i, parent->ValueAt(i + 1, fid_));
                            }
                            parent->increase_size(-1);
                            done = true;
                        }
                        if (!done) {
                            lock_cnt--;
                            next_page->mutex_.unlock();
                            cache_manager_->unpinPage(next_page_id, done);
                        }
                    }
                    // ---------------------redistributions---------------------------
                    // at this point merging is not an option.
                    // handle redistributions for internal nodes.
                    // take the current key of the parent node add it to the right
                    // take the last key from left to the parent and it's value is the 0 value of the right
                    if (prev_page_id != INVALID_PAGE_ID && !done) {
                        // DONE------.
                        //std::cout << "------------------------- prev re\n";
                        auto *prev_page = cache_manager_->fetchPage(prev_page_id);
                        lock_cnt++;
                        // TODO: Fix Dead Lock.
                        //prev_page->mutex_.lock();
                        auto *prev = reinterpret_cast<BTreeInternalPage *>(prev_page->data_);
                        auto prev_size = prev->get_num_of_slots();
                        auto pos = prev_pos + 1;
                        auto parent_key = parent->KeyAt(pos);

                        if (!prev->TooShortBefore()) {
                            done = true;
                            // take the current key of the parent node add it to the cur
                            auto *child_page = cache_manager_->fetchPage(prev->ValueAt(prev_size - 1, fid_));
                            lock_cnt++;
                            // TODO: Fix Dead Lock.
                            // child_page->mutex_.lock();
                            auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                            child->SetParentPageId(cur_page_id);

                            cur->insert_key_at_start(parent_key, prev->ValueAt(prev_size - 1, fid_));
                            cur_size++;
                            parent->SetKeyAt(pos, prev->KeyAt(prev_size - 1));
                            prev->increase_size(-1);
                            prev_size--;
                            auto tmp_child_page_id = child->GetPageId(fid_);

                            lock_cnt--;
                            child_page->mutex_.unlock();
                            cache_manager_->unpinPage(tmp_child_page_id, true);
                        }
                        lock_cnt--;
                        prev_page->mutex_.unlock();
                        cache_manager_->unpinPage(prev_page_id, done);
                    }
                    if (next_page_id != INVALID_PAGE_ID && !done) {
                        // DONE------.
                        //std::cout << "------------------------- next re\n";
                        auto *next_page = cache_manager_->fetchPage(next_page_id);
                        lock_cnt++;
                        // TODO: Fix Dead Lock.
                        //next_page->mutex_.lock();
                        auto *next = reinterpret_cast<BTreeInternalPage *>(next_page->data_);
                        auto next_size = next->get_num_of_slots();
                        auto pos = next_pos - 1;
                        auto parent_key = parent->KeyAt(pos + 1);

                        //std::cout << "NEXT POS: " << next_pos << " parent kye: " << parent_key << std::endl;
                        std::cout << cur_size << std::endl;

                        if (!next->TooShortBefore()) {
                            std::cout << "HI\n";
                            //std::cout << "child id : " << cur->ValueAt(0) << std::endl;
                            auto *random_page = reinterpret_cast<BTreeLeafPage *>(cache_manager_->fetchPage(next->ValueAt(0, fid_)));
                            //std::cout << random_page->GetNextPageId(fid_) << std::endl;
                            done = true;
                            // tell the child who is his real father :( .
                            auto *child_page = cache_manager_->fetchPage(next->ValueAt(0, fid_));
                            lock_cnt++;
                            // TODO: Fix Dead Lock.
                            // child_page->mutex_.lock();
                            auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                            auto tmp_child_page_id = child->GetPageId(fid_);
                            child->SetParentPageId(cur_page_id);
                            // append the parent's key to the end of cur along with the first val from next
                            // then push the first key up from next to the parent
                            cur->SetKeyAt(cur_size, parent_key);
                            cur->SetValAt(cur_size, next->ValueAt(0, fid_));
                            lock_cnt--;
                            child_page->mutex_.unlock();
                            cache_manager_->unpinPage(tmp_child_page_id, true);

                            cur->increase_size(1);
                            cur_size++;
                            parent->SetKeyAt(pos + 1, next->KeyAt(1));
                            next->remove_from_start();
                            next_size--;
                            std::cout << std::endl;
                        }
                        lock_cnt--;
                        next_page->mutex_.unlock();
                        cache_manager_->unpinPage(next_page_id, done);
                    }
                    //  cache_manager_->unpinPage(parent_page_id, true);
                }

                if (!cur_unpined) {
                    lock_cnt--;
                    tmp_cur_page->mutex_.unlock();
                    cache_manager_->unpinPage(tmp_cur_id, true);
                }
            }
            // in case of the key not getting inserted because of duplication we need to clear the stack.
            while (!custom_stk.empty()) {
                lock_cnt--;
                page_deque.front()->mutex_.unlock();
                cache_manager_->unpinPage(custom_stk.front()->GetPageId(fid_), false);
                custom_stk.pop_front();
                page_deque.pop_front();
            }
            std::cout << "Lock cnt: " << lock_cnt << std::endl;
        }

        IndexIterator begin() {
            if (isEmpty()) {
                return IndexIterator(nullptr, INVALID_PAGE_ID, 0);
            }
            std::shared_lock locker(root_page_id_lock_);
            auto *root_page = cache_manager_->fetchPage(root_page_id_);
            auto *root = reinterpret_cast<BTreePage *>(root_page->data_);

            while (!root->IsLeafPage()) {
                auto *cur = reinterpret_cast<BTreeInternalPage *>(root);
                auto *next_page = cache_manager_->fetchPage(cur->ValueAt(0, fid_));
                //next_page->RLatch();
                auto *next = reinterpret_cast<BTreePage *>(next_page->data_);
                //root_page->RUnlatch();

                cache_manager_->unpinPage(cur->GetPageId(fid_), false);
                if (cur->GetPageId(fid_) == root_page_id_) {
                    locker.unlock();
                }
                root = next;
                root_page = next_page;
            }

            auto it = IndexIterator(cache_manager_, root->GetPageId(fid_), 0);
            //root_page->RUnlatch();
            cache_manager_->unpinPage(root->GetPageId(fid_), false);
            return it;
        }

        // for range queries
        IndexIterator begin(const IndexKey &key) {
            if (isEmpty()) {
                return IndexIterator(nullptr, INVALID_PAGE_ID, 0);
            }
            std::shared_lock locker(root_page_id_lock_);
            std::cerr << "YO BEGIN key CALL\n";
            auto *root_page = cache_manager_->fetchPage(root_page_id_);
            //root_page->RLatch();
            auto *root = reinterpret_cast<BTreePage *>(root_page->data_);
            while (!root->IsLeafPage()) {
                auto *cur = reinterpret_cast<BTreeInternalPage *>(root);
                auto next_page_id = cur->NextPage(key, fid_);

                auto *next_page = cache_manager_->fetchPage(next_page_id);
                //next_page->RLatch();
                auto *next = reinterpret_cast<BTreePage *>(next_page->data_);

                //root_page->RUnlatch();
                cache_manager_->unpinPage(cur->GetPageId(fid_), false);

                if (cur->GetPageId(fid_) == root_page_id_) {
                    locker.unlock();
                }

                root = next;
                root_page = next_page;
            }
            auto *tmp = reinterpret_cast<BTreeLeafPage *>(root);
            int pos = tmp->GetPos(key);
            std::cout << "size: " << tmp->get_num_of_slots() << std::endl;
            std::cout << "pos: " << pos << std::endl;
            if(pos >= tmp->get_num_of_slots()) {
                cache_manager_->unpinPage(root->GetPageId(fid_), false);
                return IndexIterator(nullptr, INVALID_PAGE_ID, 0);
            }
            auto it = IndexIterator(cache_manager_, root->GetPageId(fid_), pos);
            cache_manager_->unpinPage(root->GetPageId(fid_), false);
            //root_page->RUnlatch();
            return it;
        }


        IndexIterator end() {
            if (isEmpty()) {
                return IndexIterator(nullptr, INVALID_PAGE_ID, 0);
            }
            std::shared_lock locker(root_page_id_lock_);
            std::cout << "YO  END key CALL\n";
            auto *root_page = cache_manager_->fetchPage(root_page_id_);
            //root_page->RLatch();
            auto *root = reinterpret_cast<BTreePage *>(root_page->data_);
            while (!root->IsLeafPage()) {
                auto *cur = reinterpret_cast<BTreeInternalPage *>(root);

                auto *next_page = cache_manager_->fetchPage(cur->ValueAt(cur->get_num_of_slots() - 1, fid_));
                //next_page->RLatch();
                auto *next = reinterpret_cast<BTreePage *>(next_page->data_);

                //root_page->RUnlatch();
                cache_manager_->unpinPage(cur->GetPageId(fid_), false);

                if (cur->GetPageId(fid_) == root_page_id_) {
                    locker.unlock();
                }

                root = next;
                root_page = next_page;
            }
            auto it = IndexIterator(cache_manager_, root->GetPageId(fid_), root->get_num_of_slots());
            //root_page->RUnlatch();
            cache_manager_->unpinPage(root->GetPageId(fid_), false);
            return it;
        }

        void See(){
            auto *root = reinterpret_cast<BTreePage *>(cache_manager_->fetchPage(root_page_id_)->data_);
            ToString(root);
        }

        void ToString(BTreePage* page){
            if (page->IsLeafPage()) {
                auto *leaf = reinterpret_cast<BTreeLeafPage *>(page);
                std::cout << "Leaf Page: " << leaf->GetPageId(fid_).page_num_ << " parent: " << leaf->GetParentPageId(fid_).page_num_
                    << " next: " << leaf->GetNextPageId(fid_).page_num_ << std::endl;
                for (int i = 0; i < leaf->get_num_of_slots(); i++) {
                        leaf->KeyAt(i).print();
                }
                std::cout << std::endl;
                std::cout << std::endl;
            } else {
                auto *internal = reinterpret_cast<BTreeInternalPage *>(page);
                std::cout << "Internal Page: " << internal->GetPageId(fid_).page_num_ << " parent: " << internal->GetParentPageId(fid_).page_num_ << std::endl;
                for (int i = 0; i < internal->get_num_of_slots(); i++) {
                  internal->KeyAt(i).print();
                  std::cout << ": " << internal->ValueAt(i, fid_).page_num_ << ",";
                }
                std::cout << std::endl;
                std::cout << std::endl;
                for (int i = 0; i < internal->get_num_of_slots(); i++) {
                    ToString(reinterpret_cast<BTreePage *>(cache_manager_->fetchPage(internal->ValueAt(i, fid_))->data_));
                }
            }
            cache_manager_->unpinPage(page->GetPageId(fid_), false);
        }



    private:
        bool isEmpty() {
            if (root_page_id_ == INVALID_PAGE_ID || root_page_id_.page_num_ == -1) {
                return true;
            }
            auto raw_root = reinterpret_cast<Page*>(cache_manager_->fetchPage(root_page_id_)); 
            auto *root = reinterpret_cast<BTreePage *>(raw_root->data_);
            int sz = root->get_num_of_slots();
            cache_manager_->unpinPage(root_page_id_, false);
            return sz == 0;
        }


        CacheManager* cache_manager_ = nullptr;
        TableSchema*  index_meta_schema_ = nullptr;
        FileID fid_                  = -1;
        PageID root_page_id_         = INVALID_PAGE_ID;

        std::shared_mutex root_page_id_lock_;
};
