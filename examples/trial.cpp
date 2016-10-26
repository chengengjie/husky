#include "core/engine.hpp"

using namespace husky;

class Staff {
   public:
    using KeyT = int;
    Staff() = default;
    Staff(int key) : key_(key) {}
    const KeyT& id() const { return key_; }
   protected:
    KeyT key_;
};

class Boss {
   public:
    using KeyT = int;
    Boss() = default;
    Boss(int key) : key_(key) {}
    const KeyT& id() const { return key_; }
   protected:
    KeyT key_;
};

void job() {
    auto& staff_list = ObjListFactory::create_objlist<Staff>();
    if (Context::get_global_tid() == 0)
        for (int i=0; i<20; i++)
            staff_list.add_object(i);
    
    globalize(staff_list);
    
    auto& boss_list = ObjListFactory::create_objlist<Boss>();
    auto& ch = ChannelFactory::create_push_combined_channel<int, SumCombiner<int>>(staff_list, boss_list);
    list_execute(staff_list, [&](Staff& staff) { 
        int bid = rand()%10;
        base::log_msg("I'm staff "+std::to_string(staff.id())+". I send a message to boss "+std::to_string(bid));
        ch.push(1, bid); 
    }); // send `1` to the Boss object with ID 0
    list_execute(boss_list, [&](Boss& boss) {
        int count = ch.get(boss);
        husky::base::log_msg("I'm boss "+std::to_string(boss.id())+". I saw "+std::to_string(count)+" staff.");
    });
}

int main(int argc, char** argv) {
    init_with_args(argc, argv);
    run_job(job);
    return 0;
}
