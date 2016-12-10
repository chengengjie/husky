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
    explicit FastAsyncPushChannel(ObjList<ObjT>* objlist, const MsgT& stopMsg) : AsyncPushChannel<MsgT, ObjT>(objlist, objlist) {
        // override the definition in PushChannel
        recv_comm_handler_ = [&](const MsgT& msg, ObjT* recver_obj) {
            size_t idx = this->dst_ptr_->index_of(recver_obj);
            fast_recv_buffer_.emplace_back(idx, msg);
        };
        stop_msg = stopMsg;
    }

    FastAsyncPushChannel(const FastAsyncPushChannel&) = delete;
    FastAsyncPushChannel& operator=(const FastAsyncPushChannel&) = delete;

    FastAsyncPushChannel(FastAsyncPushChannel&&) = default;
    FastAsyncPushChannel& operator=(FastAsyncPushChannel&&) = default;

    void customized_setup() override {}
    
    void broadcast_stop_msg() {
        for (int i = 0; i < worker_info_->get_num_processes(); ++i) {
            int recv_proc_num_worker = worker_info_->get_num_local_workers(i);
            int recver_local_id_ = std::hash<KeyT>()(key) % recv_proc_num_worker;
            int recver_id = worker_info_->local_to_global_id(i, recver_local_id_);
            send_buffer_[recver_id] << 0 << stop_msg; // the key 0 is meaningless
        }
    }
    
    const std::vector<MsgT>& get(const ObjT& obj) = delete;

    const auto& get_obj_msg_pairs = []() { return fast_recv_buffer_; } 
    
    bool stop() {return stop;}

   protected:
    void clear_recv_buffer_() override {
        fast_recv_buffer_.clear();
        stop = false;
    }
    void process_bin(BinStream& bin_push) {
        while (bin_push.size() != 0) {
            typename DstObjT::KeyT key;
            bin_push >> key;
            MsgT msg;
            bin_push >> msg;

            if (msg == stop_msg) stop = true;
            else{
                DstObjT* recver_obj = this->dst_ptr_->find(key);
                size_t idx;
                if (recver_obj == nullptr) {
                    DstObjT obj(key);  // Construct obj using key only
                    idx = this->dst_ptr_->add_object(std::move(obj));
                } else {
                    idx = this->dst_ptr_->index_of(recver_obj);
                }
                if (idx >= recv_buffer_.size())
                    recv_buffer_.resize(idx + 1);
                recv_buffer_[idx].push_back(std::move(msg));
            }
        }
    }

    std::vector<std::pair<size_t, MsgT>> fast_recv_buffer_;
    MsgT stop_msg;
    bool stop = false;
};

}  // namespace husky
