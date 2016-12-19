#include "mf_base.hpp"

using Vertex = VertexPR;

void PushRelabel() {
    boost::timer::cpu_timer myTimer;
    if (print()) log_msg("Start..");

    GraphStat stat;
    LoadDIMAXCSGraph<Vertex>(stat);
    if (print()) log_msg("\tload time: "+myTimer.format(4, "%w"));
    auto& vertexList = ObjListStore::get_objlist<Vertex>("vertexList");
    auto& chFlow = ChannelStore::create_push_channel<pair<int, FlowMsgT>>
        (vertexList, vertexList, "chFlow"); // (id, flow value, height) 
    auto& chHeight = ChannelStore::create_push_channel<pair<int, int>>
        (vertexList, vertexList, "chHeight"); // (id, height) 

    lib::Aggregator<int> updated; 
    updated.to_reset_each_iter();
    
    // Init preflow
    myTimer.start();
    list_execute(vertexList, {}, {&chFlow, &chHeight}, [&](Vertex& v){
        v.excess = 0;
        if (v.id() == stat.srcV){
            v.height = stat.vNum;
            for (auto& c : v.caps) {
                v.preflows.emplace(c.first, FlowT(c.second, c.second, 0));
                chFlow.push({v.id(), FlowMsgT(c.second, stat.vNum)}, c.first);
            }
            updated.update(1);
        }
        else{
            v.height = 0;
            for (auto& c : v.caps)
                v.preflows.emplace(c.first, FlowT(c.second, 0, 0)); // all out edges should be here
        }
    });
    lib::AggregatorFactory::sync();

    // Main loop
    int iter=0;
    while (updated.get_value() > 0) {
        list_execute(vertexList, {&chFlow, &chHeight}, {&chFlow, &chHeight}, [&](Vertex& v){
            // recv push values
            auto& inflows = chFlow.get(v);
            for (auto& f : inflows) v.excess += f.second.v; // to push out
            if (v.id() == stat.dstV || v.id() == stat.srcV) {
                log_msg("v"+to_string(v.id()+1)+": excess="+to_string(v.excess)+" height="+to_string(v.height));
                return;
            }
            for (auto& f : inflows) {
                auto it = v.preflows.find(f.first);
                auto& fm = f.second;
                if (it != v.preflows.end()) {
                    it->second.fm.v -= fm.v; // minus means incoming
                    it->second.fm.h = fm.h;
                }
                else v.preflows.emplace(f.first, FlowT(0, -fm.v, fm.h)); // incoming has 0 cap
            }
            auto& heights = chHeight.get(v);
            for (auto& h : heights) {
                auto it = v.preflows.find(h.first);
                it->second.fm.h = h.second;
            }
            //log_msg("v"+to_string(v.id()+1)+": excess="+to_string(v.excess)+" height="+to_string(v.height));
            if (v.excess > 0) updated.update(1);

            // calc push values
            unordered_map<int, int> toPush; // (dst, val)
            for (auto& pf : v.preflows) {
                if (v.excess == 0) break;
                auto& f = pf.second;
                if (v.height <= f.fm.h) continue;
                int flowVal = min(v.excess, f.c - f.fm.v);
                if (flowVal==0) continue;
                f.fm.v += flowVal;
                v.excess -= flowVal;
                toPush.emplace(pf.first, flowVal);
            }

            // relabel if necessary
            // if relabelled, able to send out in next round (not this round!)
            bool relabelled = false;
            if (v.excess > 0) {
                relabelled = true;
                int minH = numeric_limits<int>::max();
                for (auto& pflow : v.preflows){
                    auto& pf = pflow.second;
                    if (pf.c > pf.fm.v && minH > pf.fm.h) minH = pf.fm.h;
                }
                assert(v.height != numeric_limits<int>::max());
                v.height = minH+1;
            }

            // send out push values & label updates
            for (auto& push : toPush) {
                chFlow.push({v.id(), FlowMsgT(push.second, v.height)}, push.first);
            }
            if (relabelled) {
                for (auto& pf : v.preflows) {
                    if (toPush.find(pf.first) == toPush.end()) continue;
                    chHeight.push({v.id(), v.height}, pf.first);
                }
            }
        });
        lib::AggregatorFactory::sync();
        if (print()) log_msg("iter"+to_string(iter++)+": #updated="+to_string(updated.get_value())+"\n");
    }

    if (print()) log_msg("\tcalc time: "+myTimer.format(4, "%w"));
}
