#include "mf_base.hpp"

using Vertex = VertexEK;

int BFS_pref(const GraphStat& stat, int verbose=1){
    boost::timer::cpu_timer myTimer;
    auto& vertexList = ObjListStore::get_objlist<Vertex>("vertexList");
    auto& chV2V = ChannelStore::
        get_push_combined_channel<pair<int, vector<int>>, KeyMinCombiner<int, vector<int>>, Vertex>
        ("chV2V"); // msg: (dist, prefix)
    auto& chT2V = ChannelStore::
        get_push_channel<pair<int, int>, Vertex>
        ("chT2V"); // msg: (precessor, successor)
    lib::Aggregator<int> visited, dstVisited;
    visited.to_reset_each_iter();
    lib::Aggregator<int> minCap(numeric_limits<int>::max(), 
        [](int& a, const int& b){ if (b<a) a=b; },
        [](int& a){ a = numeric_limits<int>::max(); });

    // Init BFS
    list_execute(vertexList, [&](Vertex& v) {
        if (v.id()==stat.srcV) {
            v.dist = 0;
            visited.update(1);
            for (auto a : v.resCaps) chV2V.push({v.dist+1, {v.id()}}, a.first); // use unit-length edge in finding shortest path
        }
        else v.dist = -1;
    });
    lib::AggregatorFactory::sync();

    if (print() && verbose>=2) log_msg("Init time: "+myTimer.format(4,"%w"));
    // BFS
    int iter=0;
    while (dstVisited.get_value() == 0) {
        list_execute(vertexList, [&](Vertex& v) {
            if (v.dist!=-1 || !chV2V.has_msgs(v)) return;
            visited.update(1);
            if (v.id() == stat.dstV) {
                // update
                auto& msg = chV2V.get(v);
                v.dist = msg.first;
                dstVisited.update(1); // ask to stop
                // notify
                auto& p = msg.second;
                int np = p.size();
                if (np > 1) {
                    chT2V.push({-1, p[1]}, p[0]);
                    for (int i=1; i<np-1; ++i) chT2V.push({p[i-1],p[i+1]}, p[i]); // notify i-th vertex with its pre & suc
                    chT2V.push({p[np-2],v.id()}, p[np-1]);
                }
                else {
                    assert(np==1);
                    chT2V.push({-1, v.id()}, p[0]);
                }
                // print
                if (verbose>=1){
                    string path;
                    for (auto u : p) path += to_string(u)+"->";
                    path += to_string(v.id());
                    log_msg("shortest path ("+to_string(v.dist)+"): "+path);
                }
            }
            else{
                auto msg = chV2V.get(v);
                v.dist = msg.first;
                msg.first = v.dist+1;
                int pre = msg.second.back();
                msg.second.push_back(v.id());
                for (auto a : v.resCaps) chV2V.push(msg, a.first);
            }
        });
        lib::AggregatorFactory::sync();
        ++iter;
        if (print() && verbose>=3) log_msg("iter: "+to_string(iter)+", visited: "+to_string(visited.get_value())
                +", wall time: "+myTimer.format(4, "%w"));
        if (visited.get_value()==0) return 0;
    }
    if (print() && verbose>=2) log_msg("BFS time: "+myTimer.format(4,"%w"));
    
    // Update graph
    unordered_map<int, pair<int, int>> locEdges; // id -> (pre, suc) i.e. (back, for)
    // get the flow value
    list_execute(vertexList, [&](Vertex& v) {
        auto msg = chT2V.get(v);
        if (msg.size()== 0) return;
        assert(msg.size()==1);
        int suc = msg[0].second;
        auto it = v.resCaps.find(suc);
        assert(it != v.resCaps.end());
        minCap.update(it->second);
        //log_msg("for v"+to_string(v.id())+", suc="+to_string(suc)+", cap="+to_string(it->second));
        locEdges.emplace(v.id(), msg[0]);
    });
    lib::AggregatorFactory::sync();
    if (print() && verbose>=1) log_msg("augmented flow: " + to_string(minCap.get_value()));
    // update edges
    list_execute(vertexList, [&](Vertex& v) {
        auto it = locEdges.find(v.id());
        if (it == locEdges.end()) return;
        int pre = it->second.first, suc = it->second.second;
        // pre
        v.resCaps[pre] += minCap.get_value(); // [pre] may not exist
        // suc
        auto iSuc = v.resCaps.find(suc); // definitely exist
        iSuc->second -= minCap.get_value();
        //log_msg(to_string(v.id())+": "+to_string(iSuc->second));
        assert(iSuc->second >= 0);
        if (iSuc->second == 0) v.resCaps.erase(iSuc);
    });
    if (print() && verbose>=2) log_msg("Update time: "+myTimer.format(4,"%w"));

    return minCap.get_value();
}

void EdmondsKarpPrefix() {
    boost::timer::cpu_timer myTimer;
    if (print()) log_msg("Start..");

    GraphStat stat;
    LoadDIMAXCSGraph<Vertex>(stat);
    if (print()) log_msg("\ttime: "+myTimer.format(4, "%w"));
    auto& vertexList = ObjListStore::get_objlist<Vertex>("vertexList");
    list_execute(vertexList, [&](Vertex& v) {
        for (const auto& a:v.caps) v.resCaps.insert(a);
    });
    auto& chV2V = ChannelStore::
        create_push_combined_channel<pair<int, vector<int>>, KeyMinCombiner<int, vector<int>>>
        (vertexList, vertexList, "chV2V"); // msg: (dist, prefix)
    auto& chT2V = ChannelStore::
        create_push_channel<pair<int, int>>
        (vertexList, vertexList, "chT2V"); // msg: (precessor, successor)

    int flow, totFlow=0, iter=0;
    do{
        flow = BFS_pref(stat);
        totFlow += flow;
        if (print())
            log_msg("BFS iter: "+to_string(++iter)+", tot flow: "+to_string(totFlow)
                +", time: "+myTimer.format(4, "%w")+"\n");
    }
    while(flow>0);
}
