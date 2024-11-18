#pragma once
#include <shared_mutex>
#include <deque>
#include "cache_manager.cpp"
#include "page.cpp"
#include "record.cpp"
#include "free_space_map.cpp"
#include "btree_page.cpp"
#include "btree_leaf_page.cpp"
#include "btree_internal_page.cpp"




class BTreeIndex {
    public:
        BTreeIndex(CacheManager* cm, PageID root_page_id, int leaf_max_size, int internal_max_size):
            cache_manager_(cm),
            root_page_id_(root_page_id),
            leaf_max_size_(leaf_max_size),
            internal_max_size_(internal_max_size)
    {}
        ~BTreeIndex(){}


        void SetRootPageId(PageID root_page_id, int insert_record = 0) {
            std::unique_lock locker(this->root_page_id_lock_);
            root_page_id_ = root_page_id;
            // TODO: persist the b tree to disk.
            //UpdateRootPageId(insert_record);
        }

        // true means value is returned.
        bool GetValue(IndexKey &key, std::vector<RecordID> *result) {
            // read lock on the root_page_id_
            // std::cerr << "GET VALUE CALL\n";
            std::shared_lock locker(root_page_id_lock_);
            // std::cerr << "HI searching for: " << key << std::endl;
            if (root_page_id_ == INVALID_PAGE_ID) {
                return false;
            }
            auto *start_page = cache_manager_->fetchPage(root_page_id_);
            start_page->mutex_.lock_shared();
            auto *start = reinterpret_cast<BTreePage *>(start_page->data_);
            while (!start->IsLeafPage()) {
                auto *cur = reinterpret_cast<BTreeInternalPage *>(start);
                auto page_id = cur->NextPage(key);
                auto *tmp = start_page;
                auto tmp_page_id = start->GetPageId();

                start_page = cache_manager_->fetchPage(page_id);
                start_page->mutex_.lock_shared();
                start = reinterpret_cast<BTreePage *>(start_page->data_);

                tmp->mutex_.unlock_shared();
                cache_manager_->unpinPage(tmp_page_id, false);
                if (tmp_page_id == root_page_id_) {
                    locker.unlock();
                }
            }
            auto *leaf = reinterpret_cast<BTreeLeafPage *>(start);
            auto status = leaf->GetValue(key, result);

            start_page->mutex_.unlock_shared();
            cache_manager_->unpinPage(start->GetPageId(), false);
            return status;
        }



        // return true if inserted successfully.
        bool Insert(const IndexKey &key, const RecordID &value) {
            std::unique_lock locker(root_page_id_lock_);
            // std::cerr << "inserting: " << key << std::endl;
            std::deque<Page *> page_deque;
            BTreePage *root;
            if (root_page_id_ == INVALID_PAGE_ID) {
                // if no root then the new root will be a treated as a leaf node.
                auto tmp_root_id = root_page_id_;

                auto *leaf_page = cache_manager_->newPage(tmp_root_id.file_name_);
                // write latch.
                leaf_page->mutex_.lock();
                page_deque.push_back(leaf_page);
                auto *leaf = reinterpret_cast<BTreeLeafPage *>(leaf_page->data_);

                leaf->Init(tmp_root_id, INVALID_PAGE_ID, leaf_max_size_);
                leaf->SetPageType(BTreePageType::LEAF_PAGE);
                root = leaf;
                SetRootPageId(tmp_root_id, 1);
            } else {
                auto *root_page = cache_manager_->fetchPage(root_page_id_);
                // write latch.
                root_page->mutex_.lock();
                page_deque.push_back(root_page);
                root = reinterpret_cast<BTreePage *>(root_page->data_);
            }
            // traverse the tree untill you find the leaf node
            // and keep track of the closest empty pointer on a stack in case of a cascading split.
            std::deque<BTreePage *> custom_stk;
            custom_stk.push_back(root);
            while (!custom_stk.back()->IsLeafPage()) {
                auto *tmp = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                auto page_id = tmp->NextPage(key);

                auto *ptr_page = cache_manager_->fetchPage(page_id);
                // write latch.
                ptr_page->mutex_.lock();
                auto *ptr = reinterpret_cast<BTreePage *>(ptr_page->data_);

                bool is_full;
                if (ptr->IsLeafPage()) {
                    auto *place_holder = reinterpret_cast<BTreeLeafPage *>(ptr);
                    is_full = place_holder->IsFull();
                } else {
                    auto *place_holder = reinterpret_cast<BTreeInternalPage *>(ptr);
                    is_full = place_holder->IsFull();
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
                        if (cur->GetPageId() == root_page_id_) {
                            locker.unlock();
                        }
                        cache_manager_->unpinPage(cur->GetPageId(), false);
                    }
                }
                page_deque.push_back(ptr_page);
                custom_stk.push_back(ptr);
            }
            // now we have a stack of the nodes that we would need in the worst case which is splitting all over to the top
            // and the leaf node is at the top of the stack.

            // start by getting the top node best case: it's not full so just insert the "current key, value pair"
            // on case of a split create a new node of the same type split the keys between them
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
                    bool full = cur->IsFull();
                    inserted = cur->Insert(current_key, current_value);
                    if (!full || !inserted) {
                        break;
                    }
                    // in case of non root splits:
                    auto new_page_id = INVALID_PAGE_ID;
                    auto *new_page_raw = cache_manager_->newPage(new_page_id.file_name_);
                    // write latch.
                    new_page_raw->mutex_.lock();
                    auto *new_page = reinterpret_cast<BTreeLeafPage *>(new_page_raw->data_);
                    new_page->Init(new_page_id, cur->GetParentPageId(), cur->GetMaxSize());
                    new_page->SetPageType(BTreePageType::LEAF_PAGE);
                    int md = std::ceil(static_cast<float>(cur->GetMaxSize()) / 2);
                    md--;
                    for (int i = md + 1, j = 0; i < cur->GetSize(); i++, j++) {
                        new_page->SetKeyAt(j, cur->KeyAt(i));
                        new_page->SetValAt(j, cur->ValAt(i));
                        new_page->IncreaseSize(1);
                    }
                    cur->SetSize(md + 1);
                    IndexKey middle_key = cur->KeyAt(md);

                    current_key = middle_key;
                    current_internal_value = new_page_id;

                    // the edge case of the root being the current and the root is not empty
                    // we add two nodes inestead of just one.
                    // the splitted node and the new root.
                    if (cur->IsRootPage()) {
                        // create a new root
                        auto tmp_root_id = root_page_id_;
                        auto *new_root_raw = cache_manager_->newPage(tmp_root_id.file_name_);
                        new_root_raw->mutex_.lock();
                        auto *new_root = reinterpret_cast<BTreeInternalPage *>(new_root_raw->data_);
                        new_root->Init(tmp_root_id, INVALID_PAGE_ID, internal_max_size_);
                        new_root->SetPageType(BTreePageType::INTERNAL_PAGE);

                        // every root starts with only 1 key and 2 pointers.
                        new_root->SetValAt(0, cur->GetPageId());
                        new_root->SetKeyAt(1, current_key);
                        new_root->SetValAt(1, new_page_id);
                        new_root->SetSize(2);
                        SetRootPageId(tmp_root_id);
                        new_root_raw->mutex_.unlock();
                        cache_manager_->unpinPage(root_page_id_, true);

                        cur->SetParentPageId(root_page_id_);
                        new_page->SetParentPageId(root_page_id_);
                    }
                    new_page->SetNextPageId(cur->GetNextPageId());
                    cur->SetNextPageId(new_page_id);

                    // unpin the current_page and the new_page we don't need them any more and they are dirty.
                    page_deque.back()->mutex_.unlock();
                    new_page_raw->mutex_.unlock();
                    cache_manager_->unpinPage(cur->GetPageId(), true);
                    cache_manager_->unpinPage(new_page->GetPageId(), true);

                    custom_stk.pop_back();
                    page_deque.pop_back();
                } else {
                    auto *cur = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                    // if it's empty then add the key
                    bool full = cur->IsFull();
                    if (!full) {
                        inserted = cur->Insert(current_key, current_internal_value);
                        break;
                    }

                    // in case of non root splits:
                    auto new_page_id = INVALID_PAGE_ID;
                    auto *new_page_raw = cache_manager_->newPage(new_page_id.file_name_);
                    new_page_raw->mutex_.lock();
                    auto *new_page = reinterpret_cast<BTreeInternalPage *>(new_page_raw->data_);
                    new_page->Init(new_page_id, cur->GetParentPageId(), cur->GetMaxSize());
                    new_page->SetPageType(BTreePageType::INTERNAL_PAGE);
                    int md = std::ceil(static_cast<float>(cur->GetMaxSize() + 1) / 2);
                    int pos = cur->InsertionPosition(current_key);
                    int inserted_on_new = 0;  // 1 => on new page, 0 => no insertion, -1 => cur page.

                    if (pos > md) {
                        new_page->SetValAt(0, cur->ValueAt(md));
                        inserted_on_new = 1;
                    } else if (pos < md) {
                        md--;
                        new_page->SetValAt(0, cur->ValueAt(md));
                        inserted_on_new = -1;
                    } else {
                        new_page->SetValAt(0, current_internal_value);
                    }
                    auto *tmp1_page = cache_manager_->fetchPage(new_page->ValueAt(0));
                    tmp1_page->mutex_.lock();
                    auto *tmp1 = reinterpret_cast<BTreeInternalPage *>(tmp1_page->data_);
                    tmp1->SetParentPageId(new_page_id);
                    tmp1_page->mutex_.unlock();
                    cache_manager_->unpinPage(tmp1->GetPageId(), true);
                    // cur key = 3, cur val = 4.
                    // pos > md => md = 2
                    // pos < md => md = 1
                    // pos == md => md = 2 but not need to skip a the mid we already are skipping it.
                    int inc = 1;
                    if (inserted_on_new == 0) {
                        inc = 0;
                    }
                    for (int i = md + inc, j = 1; i < cur->GetSize(); i++, j++) {
                        new_page->SetKeyAt(j, cur->KeyAt(i));
                        new_page->SetValAt(j, cur->ValueAt(i));
                        new_page->IncreaseSize(1);

                        auto *tmp_page = cache_manager_->fetchPage(new_page->ValueAt(i));
                        tmp_page->mutex_.lock();
                        auto *tmp = reinterpret_cast<BTreeInternalPage *>(tmp_page->data_);
                        tmp->SetParentPageId(new_page_id);
                        tmp_page->mutex_.unlock();
                        cache_manager_->unpinPage(tmp->GetPageId(), true);
                    }
                    cur->SetSize(md);
                    IndexKey middle_key = current_key;
                    if (inserted_on_new != 0) {
                        middle_key = cur->KeyAt(md);
                    }
                    if (inserted_on_new == -1) {
                        cur->Insert(current_key, current_internal_value);

                        auto *tmp2_page = cache_manager_->fetchPage(current_internal_value);
                        tmp2_page->mutex_.lock();
                        auto *tmp2 = reinterpret_cast<BTreeInternalPage *>(tmp2_page->data_);

                        tmp2->SetParentPageId(cur->GetPageId());

                        tmp2_page->mutex_.lock();
                        cache_manager_->unpinPage(tmp2->GetPageId(), true);
                    } else if (inserted_on_new == 1) {
                        new_page->Insert(current_key, current_internal_value);

                        auto *tmp2_page = cache_manager_->fetchPage(current_internal_value);
                        tmp2_page->mutex_.lock();
                        auto *tmp2 = reinterpret_cast<BTreeInternalPage *>(tmp2_page->data_);

                        tmp2->SetParentPageId(new_page_id);

                        tmp2_page->mutex_.unlock();
                        cache_manager_->unpinPage(tmp2->GetPageId(), true);
                    }

                    current_key = middle_key;
                    current_internal_value = new_page_id;
                    // the edge case of the root being the current and the root is not empty
                    // we add two nodes inestead of just one.
                    // the splitted node and the new root.
                    if (cur->IsRootPage()) {
                        // create a new root
                        auto tmp_root_id = root_page_id_;
                        auto *new_root_raw = cache_manager_->newPage(tmp_root_id.file_name_);
                        new_root_raw->mutex_.lock();
                        auto *new_root = reinterpret_cast<BTreeInternalPage *>(new_root_raw->data_);
                        new_root->Init(tmp_root_id, INVALID_PAGE_ID, internal_max_size_);
                        new_root->SetPageType(BTreePageType::INTERNAL_PAGE);

                        // every root starts with only 1 key and 2 pointers.
                        new_root->SetValAt(0, cur->GetPageId());
                        new_root->SetKeyAt(1, current_key);
                        new_root->SetValAt(1, current_internal_value);
                        new_root->SetSize(2);

                        SetRootPageId(tmp_root_id);
                        new_root_raw->mutex_.unlock();
                        cache_manager_->unpinPage(root_page_id_, true);

                        cur->SetParentPageId(root_page_id_);
                        new_page->SetParentPageId(root_page_id_);
                    }
                    // unpin the current_page and the new_page we don't need them any more and they are dirty.
                    page_deque.back()->mutex_.unlock();
                    new_page_raw->mutex_.unlock();
                    cache_manager_->unpinPage(cur->GetPageId(), true);
                    cache_manager_->unpinPage(new_page->GetPageId(), true);

                    custom_stk.pop_back();
                    page_deque.pop_back();
                }
            }
            // in case of the key not getting inserted because of duplication we need to clear the stack.
            while (!custom_stk.empty()) {
                page_deque.front()->mutex_.unlock();
                cache_manager_->unpinPage(custom_stk.front()->GetPageId(), true);
                custom_stk.pop_front();
                page_deque.pop_front();
            }
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
            root_page->mutex_.lock();
            std::cout << "YO grapped the lock\n";
            auto *root = reinterpret_cast<BTreePage *>(root_page->data_);
            // traverse the tree untill you find the leaf node
            // and keep track of the closest pointer with more than m/2 on a stack in case of a cascading merge.
            custom_stk.push_back(root);
            page_deque.push_back(root_page);
            while (!custom_stk.back()->IsLeafPage()) {
                auto *tmp = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                auto page_id = tmp->NextPage(key);

                auto *ptr_page = cache_manager_->fetchPage(page_id);
                lock_cnt++;
                ptr_page->mutex_.lock();
                auto *ptr = reinterpret_cast<BTreePage *>(ptr_page->data_);

                page_deque.push_back(ptr_page);
                custom_stk.push_back(ptr);
            }

            while (!custom_stk.empty()) {
                bool cur_unpined = false;
                auto tmp_cur_id = custom_stk.back()->GetPageId();
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
                        auto tmp_id = cur->GetPageId();
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
                    auto next_page_id = cur->GetNextPageId();
                    auto cur_size = cur->GetSize();
                    auto cur_page_id = cur->GetPageId();
                    auto parent_page_id = cur->GetParentPageId();
                    // check it out later IMPORTANT.
                    // auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
                    auto *parent = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());
                    //      auto pos = parent->InsertionPosition(key, comparator_);
                    //     pos--;
                    PageID prev_page_id = INVALID_PAGE_ID;
                    auto prev_pos = parent->PrevPageOffset(key);
                    if (prev_pos != -1) {
                        prev_page_id = parent->ValueAt(prev_pos);
                    }
                    // check it out later IMPORTANT.
                    auto done = false;
                    if (prev_page_id != INVALID_PAGE_ID && !done) {
                        //std::cout << "------------------------- leaf prev merge\n";
                        auto *prev_page = cache_manager_->fetchPage(prev_page_id);
                        lock_cnt++;
                        prev_page->mutex_.lock();
                        auto *prev = reinterpret_cast<BTreeLeafPage *>(prev_page->data_);
                        auto prev_size = prev->GetSize();
                        if (prev->GetParentPageId() == parent_page_id && prev_size + cur_size < cur->GetMaxSize()) {
                            // std::cout << cur->GetSize() << " " << cur->GetNextPageId() << std::endl;
                            done = true;
                            // merge into prev and delete cur, update the parent(remove the key-value) then break.
                            for (int i = 0; i < cur_size; i++) {
                                prev->SetKeyAt(prev_size + i, cur->KeyAt(i));
                                prev->SetValAt(prev_size + i, cur->ValAt(i));
                            }
                            prev->IncreaseSize(cur_size);
                            prev->SetNextPageId(cur->GetNextPageId());

                            lock_cnt--;
                            cur_page->mutex_.unlock();
                            cache_manager_->unpinPage(cur_page_id, true);
                            cache_manager_->deletePage(cur_page_id);
                            cur_unpined = true;

                            // clear parent
                            auto pos = prev_pos + 1;
                            for (int i = pos; i < parent->GetSize() - 1; i++) {
                                parent->SetKeyAt(i, parent->KeyAt(i + 1));
                                parent->SetValAt(i, parent->ValueAt(i + 1));
                            }
                            parent->IncreaseSize(-1);
                        }
                        //std::cout << "------------------------- leaf prev merge everything is just fine\n";
                        //std::cout << prev->GetPageId() << " " << prev_page_id << std::endl;
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
                        next_page->mutex_.lock();
                        auto *next = reinterpret_cast<BTreeLeafPage *>(next_page->data_);
                        auto next_size = next->GetSize();
                        bool got_in = false;
                        if (next->GetParentPageId() == parent_page_id && next_size + cur_size < cur->GetMaxSize()) {
                            //std::cout << next_page_id << " " << next->GetNextPageId() << std::endl;
                            done = true;
                            got_in = true;
                            for (int i = 0; i < next_size; i++) {
                                cur->SetKeyAt(cur_size + i, next->KeyAt(i));
                                cur->SetValAt(cur_size + i, next->ValAt(i));
                            }
                            cur->IncreaseSize(next_size);
                            cur->SetNextPageId(next->GetNextPageId());

                            lock_cnt--;
                            next_page->mutex_.unlock();
                            cache_manager_->unpinPage(next_page_id, true);
                            cache_manager_->deletePage(next_page_id);

                            // clear parent
                            auto pos = parent->NextPageOffset(key);
                            for (int i = pos; i < parent->GetSize() - 1; i++) {
                                parent->SetKeyAt(i, parent->KeyAt(i + 1));
                                parent->SetValAt(i, parent->ValueAt(i + 1));
                            }
                            parent->IncreaseSize(-1);
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
                        prev_page->mutex_.lock();
                        auto *prev = reinterpret_cast<BTreeLeafPage *>(prev_page->data_);
                        auto prev_size = prev->GetSize();
                        bool got_in = false;

                        if (prev->GetParentPageId() == parent_page_id && !prev->TooShortBefore()) {
                            done = true;
                            got_in = true;
                            // take last key from prev insert it into current then update parent.
                            cur->Insert(prev->KeyAt(prev_size), prev->ValAt(prev_size));
                            prev->IncreaseSize(-1);
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
                        next_page->mutex_.lock();
                        auto *next = reinterpret_cast<BTreeLeafPage *>(next_page->data_);
                        auto next_size = next->GetSize();
                        auto got_in = false;

                        if (next->GetParentPageId() == parent_page_id && !next->TooShortBefore()) {
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
                    //      buffer_pool_manager_->UnpinPage(parent_page_id, true);
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
                        cache_manager_->unpinPage(cur->GetPageId(), true);
                        cur_unpined = true;
                        break;
                    }
                    if (cur->IsRootPage()) {
                        SetRootPageId(cur->ValueAt(0));

                        auto *child_page = cache_manager_->fetchPage(cur->ValueAt(0));
                        lock_cnt++;
                        child_page->mutex_.lock();
                        auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                        child->SetParentPageId(INVALID_PAGE_ID);
                        lock_cnt--;
                        child_page->mutex_.unlock();
                        cache_manager_->unpinPage(cur->ValueAt(0), true);

                        lock_cnt--;
                        cur_page->mutex_.unlock();
                        cache_manager_->unpinPage(cur->GetPageId(), true);
                        cache_manager_->deletePage(cur->GetPageId());
                        cur_unpined = true;
                        break;
                    }
                    // first merges:
                    // no prev and next pointer so need to get them from the parent.
                    // may as well just fetch the parent right away.
                    auto cur_page_id = cur->GetPageId();
                    // auto parent_page_id = cur->GetParentPageId();
                    auto cur_size = cur->GetSize();
                    // IMPORTANT.
                    auto *parent = reinterpret_cast<BTreeInternalPage *>(custom_stk.back());

                    auto parent_size = parent->GetSize();
                    bool done = false;
                    auto prev_page_id = INVALID_PAGE_ID;
                    auto next_page_id = INVALID_PAGE_ID;
                    auto prev_pos = parent->PrevPageOffset(key);
                    auto next_pos = parent->NextPageOffset(key);

                    if (prev_pos != -1) {
                        prev_page_id = parent->ValueAt(prev_pos);
                    }
                    if (next_pos < parent_size) {
                        next_page_id = parent->ValueAt(next_pos);
                    }

                    if (prev_page_id != INVALID_PAGE_ID && !done) {
                        // DONE----------
                        //std::cout << "------------------------- prev merge\n";
                        // merge into prev then delete cur.
                        auto *prev_page = cache_manager_->fetchPage(prev_page_id);
                        lock_cnt++;
                        prev_page->mutex_.lock();
                        auto *prev = reinterpret_cast<BTreeInternalPage *>(prev_page->data_);
                        auto prev_size = prev->GetSize();
                        if (prev_size + cur_size <= prev->GetMaxSize()) {
                            auto pos = prev_pos + 1;
                            // add the key from the parent and the first pointer from current as a new key-value pair.
                            auto parent_key = parent->KeyAt(pos);
                            prev->SetKeyAt(prev_size, parent_key);
                            prev->SetValAt(prev_size, cur->ValueAt(0));
                            prev->IncreaseSize(1);
                            prev_size++;

                            for (int i = 0; i < cur->GetSize(); i++) {
                                auto child_page = cache_manager_->fetchPage(cur->ValueAt(i));
                                lock_cnt++;
                                child_page->mutex_.lock();
                                auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                                auto tmp_child_page_id = child->GetPageId();

                                child->SetParentPageId(prev_page_id);

                                lock_cnt--;
                                child_page->mutex_.unlock();
                                cache_manager_->unpinPage(tmp_child_page_id, true);
                            }

                            for (int i = 1; i < cur_size; i++) {
                                prev->SetKeyAt(prev_size + (i - 1), cur->KeyAt(i));
                                prev->SetValAt(prev_size + (i - 1), cur->ValueAt(i));
                            }
                            prev->IncreaseSize((cur_size - 1));
                            lock_cnt--;
                            cur_page->mutex_.unlock();
                            cache_manager_->unpinPage(cur_page_id, true);
                            cache_manager_->deletePage(cur_page_id);
                            cur_unpined = true;
                            // clear parent
                            for (int i = pos; i < parent->GetSize() - 1; i++) {
                                parent->SetKeyAt(i, parent->KeyAt(i + 1));
                                parent->SetValAt(i, parent->ValueAt(i + 1));
                            }
                            parent->IncreaseSize(-1);
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
                        next_page->mutex_.lock();
                        auto *next = reinterpret_cast<BTreeInternalPage *>(next_page->data_);
                        auto next_size = next->GetSize();
                        auto parent_key = parent->KeyAt(next_pos);
                        if (next_size + cur_size <= cur->GetMaxSize()) {
                            // add the key from the parent and the first pointer from next as a new key-value pair.
                            cur->SetKeyAt(cur_size, parent_key);
                            cur->SetValAt(cur_size, next->ValueAt(0));
                            //std::cout << next->ValueAt(1) << std::endl;
                            for (int i = 0; i < next->GetSize(); i++) {
                                auto *child_page = cache_manager_->fetchPage(next->ValueAt(i));
                                lock_cnt++;
                                child_page->mutex_.lock();
                                auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                                auto tmp_child_page_id = child->GetPageId();
                                child->SetParentPageId(cur_page_id);
                                lock_cnt--;
                                child_page->mutex_.unlock();
                                cache_manager_->unpinPage(tmp_child_page_id, true);
                            }
                            cur->IncreaseSize(1);
                            cur_size++;

                            for (int i = 1; i < next_size; i++) {
                                cur->SetKeyAt(cur_size + (i - 1), next->KeyAt(i));
                                cur->SetValAt(cur_size + (i - 1), next->ValueAt(i));
                            }
                            cur->IncreaseSize((next_size - 1));
                            cur_size += (next_size - 1);
                            lock_cnt--;
                            next_page->mutex_.unlock();
                            cache_manager_->unpinPage(next_page_id, true);
                            cache_manager_->deletePage(next_page_id);
                            // clear parent
                            for (int i = next_pos; i < parent->GetSize() - 1; i++) {
                                parent->SetKeyAt(i, parent->KeyAt(i + 1));
                                parent->SetValAt(i, parent->ValueAt(i + 1));
                            }
                            parent->IncreaseSize(-1);
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
                        prev_page->mutex_.lock();
                        auto *prev = reinterpret_cast<BTreeInternalPage *>(prev_page->data_);
                        auto prev_size = prev->GetSize();
                        auto pos = prev_pos + 1;
                        auto parent_key = parent->KeyAt(pos);

                        if (!prev->TooShortBefore()) {
                            done = true;
                            // take the current key of the parent node add it to the cur
                            auto *child_page = cache_manager_->fetchPage(prev->ValueAt(prev_size - 1));
                            lock_cnt++;
                            child_page->mutex_.lock();
                            auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                            child->SetParentPageId(cur_page_id);

                            cur->InsertKeyAtStart(parent_key, prev->ValueAt(prev_size - 1));
                            cur_size++;
                            parent->SetKeyAt(pos, prev->KeyAt(prev_size - 1));
                            prev->IncreaseSize(-1);
                            prev_size--;
                            auto tmp_child_page_id = child->GetPageId();

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
                        next_page->mutex_.lock();
                        auto *next = reinterpret_cast<BTreeInternalPage *>(next_page->data_);
                        auto next_size = next->GetSize();
                        auto pos = next_pos - 1;
                        auto parent_key = parent->KeyAt(pos + 1);

                        //std::cout << "NEXT POS: " << next_pos << " parent kye: " << parent_key << std::endl;
                        std::cout << cur_size << std::endl;

                        if (!next->TooShortBefore()) {
                            std::cout << "HI\n";
                            //std::cout << "child id : " << cur->ValueAt(0) << std::endl;
                            auto *random_page = reinterpret_cast<BTreeLeafPage *>(cache_manager_->fetchPage(next->ValueAt(0)));
                            //std::cout << random_page->GetNextPageId() << std::endl;
                            done = true;
                            // tell the child who is his real father :( .
                            auto *child_page = cache_manager_->fetchPage(next->ValueAt(0));
                            lock_cnt++;
                            child_page->mutex_.lock();
                            auto *child = reinterpret_cast<BTreePage *>(child_page->data_);
                            auto tmp_child_page_id = child->GetPageId();
                            child->SetParentPageId(cur_page_id);
                            // append the parent's key to the end of cur along with the first val from next
                            // then push the first key up from next to the parent
                            cur->SetKeyAt(cur_size, parent_key);
                            cur->SetValAt(cur_size, next->ValueAt(0));
                            lock_cnt--;
                            child_page->mutex_.unlock();
                            cache_manager_->unpinPage(tmp_child_page_id, true);

                            cur->IncreaseSize(1);
                            cur_size++;
                            parent->SetKeyAt(pos + 1, next->KeyAt(1));
                            next->RemoveFromStart();
                            next_size--;
                            std::cout << std::endl;
                        }
                        lock_cnt--;
                        next_page->mutex_.unlock();
                        cache_manager_->unpinPage(next_page_id, done);
                    }
                    //  buffer_pool_manager_->UnpinPage(parent_page_id, true);
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
                cache_manager_->unpinPage(custom_stk.front()->GetPageId(), false);
                custom_stk.pop_front();
                page_deque.pop_front();
            }
            std::cout << "Lock cnt: " << lock_cnt << std::endl;
        }


    private:
        CacheManager* cache_manager_ = nullptr;
        PageID root_page_id_ = INVALID_PAGE_ID;
        int leaf_max_size_ = 0;
        int internal_max_size_ = 0;

        std::shared_mutex root_page_id_lock_;
};
