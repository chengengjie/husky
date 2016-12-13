#include "mf_base.hpp"

using Vertex = VertexEKPredecessor;

int BFS(const GraphStat& stat, int verbose=1){
    boost::timer::cpu_timer myTimer;
    auto& vertexList = ObjListStore::get_objlist<Vertex>("vertexList");
    //auto& chV2V = ChannelStore::
    //    get_push_combined_channel<pair<int, int>, KeyMinCombiner<int, int>, Vertex>
    //    ("chV2V"); // msg: (dist, precessor)
    lib::Aggregator<int> visited, dstVisited;
    visited.to_reset_each_iter();
    lib::Aggregator<int> minCap(numeric_limits<int>::max(), 
        [](int& a, const int& b){ if (b<a) a=b; },
        [](int& a){ a = numeric_limits<int>::max(); });

    // Init DFS
    auto& chV2V = ChannelStore::
        create_push_combined_channel<pair<int, int>, KeyMinCombiner<int, int>>
        (vertexList, vertexList, "chV2V"); // msg: (dist, precessor)
    list_execute(vertexList, {}, {&chV2V}, [&](Vertex& v) {
        if (v.id()==stat.srcV) {
            v.dist = 0;
            v.pre = -1;
            visited.update(1);
            for (auto a : v.resCaps) chV2V.push({v.dist+1, {v.id()}}, a.first); // use unit-length edge in finding shortest path
        }
        else v.dist = -1;
    });
    lib::AggregatorFactory::sync();

    if (print() && verbose>=2) log_msg("Init time: "+myTimer.format(4,"%w"));
    // DFS
    int iter=0;
    while (dstVisited.get_value() == 0) {
        list_execute(vertexList, {&chV2V}, {&chV2V}, [&](Vertex& v) {
            if (v.dist!=-1 || !chV2V.has_msgs(v)) return;
            auto& msg = chV2V.get(v);
            v.dist = msg.first;
            v.pre = msg.second;
            //log_msg(to_string(v.id())+" receive from "+to_string(v.pre));
            if (v.id() == 47 || v.id() == 52 || v.pre == 47 || v.pre == 52)
                log_msg("During DFS, the pre of "+to_string(v.id())+" is updated to "+to_string(v.pre)+" my dist is "+to_string((v.dist)));
            visited.update(1);
            if (v.id() == stat.dstV) {
                dstVisited.update(1); // ask to stop
                if (verbose>=1) cout << "shortest path (" << v.dist << "): " << v.id();
            }
            else{
                for (auto a : v.resCaps) {
                    chV2V.push({v.dist+1, v.id()}, a.first);
                    //log_msg(to_string(v.id())+" push to "+to_string(a.first));
                }
            }
        });
        lib::AggregatorFactory::sync();
        ++iter;
        if (print() && verbose>=2) log_msg("iter: "+to_string(iter)+", visited: "+to_string(visited.get_value())
                +", wall time: "+myTimer.format(4, "%w"));
        if (visited.get_value()==0) return 0;
    }
    ChannelStore::drop_channel("chV2V");
    if (print() && verbose>=2) log_msg("BFS time: "+myTimer.format(4,"%w"));

    // Init backtracking
    auto& chT2V = ChannelStore::
        create_fast_async_push_channel<int>
        (vertexList, -1, "chT2V"); // msg: (precessor, successor)
    list_execute(vertexList, {}, {&chT2V}, [&](Vertex& v) {
        if (v.id() == stat.dstV) {
            chT2V.push(v.id(), v.pre);
        }
    });
    
    // Update graph
    unordered_map<int, int> locEdges; // id -> suc i.e. for
    // notify vertices on the path
    list_execute_fast_async(vertexList, &chT2V, [&](Vertex& v, int suc) {
        auto it = v.resCaps.find(suc);
        assert(it != v.resCaps.end());
        //log_msg("for v"+to_string(v.id())+", suc="+to_string(suc)+", cap="+to_string(it->second));
        locEdges.emplace(v.id(), suc);
        if (v.pre == -1) chT2V.broadcast_stop_msg();
        else chT2V.push(v.id(), v.pre);
        if (verbose>=1) cout << "<-" << v.id();
        //log_msg("I am "+to_string(v.id())+", my pre is "+to_string(v.pre));
    });
    ChannelStore::drop_channel("chT2V"); // very important (not robust)
    // get the flow value
    list_execute(vertexList, {}, {}, [&](Vertex& v){
        auto it = locEdges.find(v.id());
        if (it == locEdges.end()) return;
        auto it2 = v.resCaps.find(it->second);
        assert(it2 != v.resCaps.end());
        minCap.update(it2->second);
    });
    lib::AggregatorFactory::sync();
    if (print() && verbose>=1) log_msg("\naugmented flow: " + to_string(minCap.get_value()));
    // update edges
    list_execute(vertexList, {}, {}, [&](Vertex& v) {
        auto it = locEdges.find(v.id());
        if (it == locEdges.end()) return;
        // pre
        v.resCaps[v.pre] += minCap.get_value(); // [pre] may not exist
        // suc
        auto iSuc = v.resCaps.find(it->second); // definitely exist
        iSuc->second -= minCap.get_value();
        assert(iSuc->second >= 0);
        if (iSuc->second == 0) v.resCaps.erase(iSuc);
    });
    if (print() && verbose>=2) log_msg("Update time: "+myTimer.format(4,"%w"));

    return minCap.get_value();
}

void EdmondsKarpPredecessor() {
    boost::timer::cpu_timer myTimer;
    if (print()) log_msg("Start..");

    GraphStat stat;
    LoadDIMAXCSGraph<Vertex>(stat);
    if (print()) log_msg("\ttime: "+myTimer.format(4, "%w"));
    auto& vertexList = ObjListStore::get_objlist<Vertex>("vertexList");
    list_execute(vertexList, [&](Vertex& v) {
        for (const auto& a:v.caps) v.resCaps.insert(a);
    });

    int flow, totFlow=0, iter=0;
    do{
        flow = BFS(stat, 2);
        totFlow += flow;
        if (print()) {
            log_msg("DFS iter: "+to_string(++iter)+", tot flow: "+to_string(totFlow)+", time: "+myTimer.format(4, "%w"));
            log_msg("");
        }
    }
    while(flow>0);
}
