// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <functional>
#include <vector>

#include "base/serialization.hpp"
#include "core/channel/channel_impl.hpp"
#include "core/channel/push_channel.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/worker_info.hpp"

namespace husky {

using base::BinStream;

template <typename MsgT, typename ObjT>
class AsyncPushChannel : public PushChannel<MsgT, ObjT> {
   public:
    explicit AsyncPushChannel(ObjList<ObjT>* objlist) : PushChannel<MsgT, ObjT>(objlist, objlist) {
        this->set_as_async_channel();
    }

    AsyncPushChannel(const AsyncPushChannel&) = delete;
    AsyncPushChannel& operator=(const AsyncPushChannel&) = delete;

    AsyncPushChannel(AsyncPushChannel&&) = default;
    AsyncPushChannel& operator=(AsyncPushChannel&&) = default;

    void out() override {
        // No increment progress id here
        int start = this->global_id_;
        for (int i = 0; i < this->send_buffer_.size(); ++i) {
            int dst = (start + i) % this->send_buffer_.size();
            if (this->send_buffer_.size() == 0)
                continue;
            this->mailbox_->send(dst, this->channel_id_, this->progress_, this->send_buffer_[dst]);
            this->send_buffer_[dst].purge();
        }
        // No send_complete here
    }

    // This is for unittest only
    void prepare_messages_test() {
        this->clear_recv_buffer_();
        while (this->mailbox_->poll_with_timeout(this->channel_id_, this->progress_, 1.0)) {
            auto bin_push = this->mailbox_->recv(this->channel_id_, this->progress_);
            this->process_bin(bin_push);
        }
        this->reset_flushed();
    }
};

template <typename MsgT, typename ObjT>
class FastAsyncPushChannel : public AsyncPushChannel<MsgT, ObjT> {
   public:
    explicit FastAsyncPushChannel(ObjList<ObjT>* objlist, const MsgT& stopMsg) : AsyncPushChannel<MsgT, ObjT>(objlist) {
        stop_msg = stopMsg;
    }

    FastAsyncPushChannel(const FastAsyncPushChannel&) = delete;
    FastAsyncPushChannel& operator=(const FastAsyncPushChannel&) = delete;

    FastAsyncPushChannel(FastAsyncPushChannel&&) = default;
    FastAsyncPushChannel& operator=(FastAsyncPushChannel&&) = default;

    void broadcast_stop_msg() {
        for (int i = 0; i < this->worker_info_->get_num_processes(); ++i) {
            //TODO: use accessor
            int recv_proc_num_worker = this->worker_info_->get_num_local_workers(i);
            //int recver_local_id_ = std::hash<typename ObjT::KeyT>()(key) % recv_proc_num_worker;
            for (int j=0; j < recv_proc_num_worker; ++j){
                int recver_id = this->worker_info_->local_to_global_id(i, j);
                this->send_buffer_[recver_id] << 0 << stop_msg; // the key 0 is meaningless
            }
        }
        // also mark itself to stop
        stop_ = true;
    }

    void prepare() override { clear_recv_buffer_(); }
    void in(BinStream& bin) override { process_bin(bin); }
    
    const std::vector<MsgT>& get(const ObjT& obj) = delete;

    const std::vector<std::pair<size_t, MsgT> >& get_obj_msg_pairs() 
        { return fast_recv_buffer_; } 
    
    bool stop() {return stop_;}

   protected:
    void clear_recv_buffer_() {
        fast_recv_buffer_.clear();
        stop_ = false;
    }
    void process_bin(BinStream& bin_push) {
        while (bin_push.size() != 0) {
            typename ObjT::KeyT key;
            bin_push >> key;
            MsgT msg;
            bin_push >> msg;

            if (msg == stop_msg) stop_ = true;
            else{
                ObjT* recver_obj = this->dst_ptr_->find(key);
                size_t idx;
                if (recver_obj == nullptr) {
                    ObjT obj(key);  // Construct obj using key only
                    idx = this->dst_ptr_->add_object(std::move(obj));
                } else {
                    idx = this->dst_ptr_->index_of(recver_obj);
                }
                //if (idx >= recv_buffer_.size())
                //    recv_buffer_.resize(idx + 1);
                //recv_buffer_[idx].push_back(std::move(msg));
                fast_recv_buffer_.emplace_back(idx, msg);
            }
        }
    }

    std::vector<std::pair<size_t, MsgT> > fast_recv_buffer_;
    MsgT stop_msg;
    bool stop_ = false;
};

}  // namespace husky
