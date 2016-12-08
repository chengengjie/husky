#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

#include "boost/tokenizer.hpp"
#include <boost/timer/timer.hpp>

using namespace std;
using namespace husky;
using base::log_msg;

class Vertex {
public:
    using KeyT = int;

    Vertex() = default;
    explicit Vertex(const KeyT& i) : vertexId(i) {}
    const KeyT& id() const { return vertexId; }

    // Serialization and deserialization
    friend BinStream& operator<<(BinStream& stream, const Vertex& v) {
        stream << v.vertexId << v.caps << v.resCaps << v.dist;
        return stream;
    }
    friend BinStream& operator>>(BinStream& stream, Vertex& v) {
        stream >> v.vertexId >> v.caps >> v.resCaps >> v.dist;
        return stream;
    }

    KeyT vertexId;
    vector<pair<KeyT, int>> caps;
    unordered_map<KeyT, int> resCaps; // (id, edge weight)
    int dist; // -1 stands for "not visited"
};

template <typename K, typename V>
struct KeyMinCombiner {
    static void combine(pair<K, V>& val, pair<K, V> const& other) { if (other.first < val.first) val = other; }
};

inline bool print() { return Context::get_global_tid()==0; }

int vertexNum, edgeNum, srcVertex, dstVertex;

void LoadDIMAXCSGraph(){
    // Read DIMAXCS file
    lib::Aggregator<int> vertexNumAgg, edgeNumAgg, srcVertexAgg, dstVertexAgg;
    auto& infmt = io::InputFormatStore::create_line_inputformat();
    infmt.set_input(Context::get_param("hdfs_input"));
    auto& vertexList = ObjListStore::create_objlist<Vertex>("vertexList");
    auto& chIn2V = ChannelStore::create_push_channel<pair<int,int>>(infmt, vertexList);
    auto parseDIMACS = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0) return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        char flag = it->at(0);
        ++it;
        if(flag == 'c' || flag == '\n'){
            return;
        }else if(flag == 'p'){
            ++it;
            int n = stoi(*it++), m = stoi(*it++);
            vertexNumAgg.update(n);
            edgeNumAgg.update(m);
        }else if(flag == 'a'){
            int u = stoi(*it++)-1, v = stoi(*it++)-1, c = stoi(*it++);
            chIn2V.push({u, c}, v);
            chIn2V.push({v, c}, u);
        }else if(flag == 'n'){
            int v = stoi(*it++)-1;
            char st = it->at(0);
            if (st == 's') srcVertexAgg.update(v);
            else if (st == 't') dstVertexAgg.update(v);
            else cerr << "invalid line for input flag: " << flag << endl;
        }else cerr << "unknown input flag: " << flag << endl;
    };
    load(infmt, parseDIMACS);
    
    // Process data
    list_execute(vertexList, [&](Vertex& v) {
        v.caps = chIn2V.get(v);
        for (const auto& a:v.caps) v.resCaps.insert(a);
    });
    globalize(vertexList);
    lib::AggregatorFactory::sync();
    vertexNum=vertexNumAgg.get_value();
    edgeNum=edgeNumAgg.get_value();
    srcVertex=srcVertexAgg.get_value();
    dstVertex=dstVertexAgg.get_value();
}

int DFS(int verbose=1){
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

    // Init DFS
    list_execute(vertexList, [&](Vertex& v) {
        if (v.id()==srcVertex) {
            v.dist = 0;
            visited.update(1);
            for (auto a : v.resCaps) chV2V.push({v.dist+1, {v.id()}}, a.first); // use unit-length edge in finding shortest path
        }
        else v.dist = -1;
    });
    lib::AggregatorFactory::sync();

    // DFS
    int iter=0;
    while (dstVisited.get_value() == 0) {
        list_execute(vertexList, [&](Vertex& v) {
            if (v.dist!=-1 || !chV2V.has_msgs(v)) return;
            if (v.id() == dstVertex) {
                // update
                auto& msg = chV2V.get(v);
                v.dist = msg.first;
                visited.update(1);
                dstVisited.update(1); // ask to stop
                // notify
                auto& p = msg.second;
                int np = p.size();
                chT2V.push({-1, p[1]}, p[0]);
                for (int i=1; i<np-1; ++i) chT2V.push({p[i-1],p[i+1]}, p[i]); // notify i-th vertex with its pre & suc
                chT2V.push({p[np-2],v.id()}, p[np-1]);
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
                visited.update(1);
                msg.first = v.dist+1;
                msg.second.push_back(v.id());
                for (auto a : v.resCaps) chV2V.push(msg, a.first);
            }
        });
        lib::AggregatorFactory::sync();
        ++iter;
        if (print() && verbose>=2) log_msg("iter: "+to_string(iter)+", visited: "+to_string(visited.get_value())
                +", wall time: "+myTimer.format(4, "%w"));
        if (visited.get_value()==0) return 0;
    }
    
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
    
    return minCap.get_value();
}

void EdmondsKarp() {
    boost::timer::cpu_timer myTimer;
    if (print()) log_msg("Start..");

    LoadDIMAXCSGraph();
    if (print()) {
        log_msg("Finish loading DIMACS graph.");
        log_msg("\tvertex num: "+to_string(vertexNum)+", edge num: "+to_string(edgeNum)
            +", src vertex: "+to_string(srcVertex)+", dst vertex: "+to_string(dstVertex)+", wall time: "+myTimer.format(4, "%w"));
    }
    auto& vertexList = ObjListStore::get_objlist<Vertex>("vertexList");
    auto& chV2V = ChannelStore::
        create_push_combined_channel<pair<int, vector<int>>, KeyMinCombiner<int, vector<int>>>
        (vertexList, vertexList, "chV2V"); // msg: (dist, prefix)
    auto& chT2V = ChannelStore::
        create_push_channel<pair<int, int>>
        (vertexList, vertexList, "chT2V"); // msg: (precessor, successor)

    int flow, totFlow=0, iter=0;
    do{
        flow = DFS();
        totFlow += flow;
        if (print()) {
            log_msg("DFS iter: "+to_string(++iter)+", tot flow: "+to_string(totFlow)+", time: "+myTimer.format(4, "%w"));
            log_msg("");
        }
    }
    while(flow>0);

    string small_graph = Context::get_param("print");
    if (small_graph == "1") {
        list_execute(vertexList, [](Vertex& v) {
            log_msg("vertex: "+to_string(v.id()) + " dist: "+to_string(v.dist));
        });
    }
}

int main(int argc, char** argv)
{
    boost::timer::cpu_timer myTimer;
    vector<string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("hdfs_input");
    args.push_back("print");
    if (init_with_args(argc, argv, args)) {
        run_job(EdmondsKarp);
        log_msg("total wall time: "+myTimer.format(4, "%w"));
        return 0;
    }
    return 1;
}
