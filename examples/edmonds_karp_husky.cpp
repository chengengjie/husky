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
    int dist = -1; // -1 stands for "not visited"
};

template <typename K, typename V>
struct KeyMinCombiner {
    static void combine(pair<K, V>& val, pair<K, V> const& other) { if (other.first < val.first) val = other; }
};

void mf() {
    int id = Context::get_global_tid();
    if (id == 0) log_msg("Start..");
    boost::timer::cpu_timer myTimer;

    // Read DIMAXCS file
    lib::Aggregator<int> vertexNumAgg, edgeNumAgg, srcVertexAgg, dstVertexAgg;
    int vertexNum, edgeNum, srcVertex, dstVertex;
    auto& infmt = io::InputFormatStore::create_line_inputformat();
    infmt.set_input(Context::get_param("hdfs_input"));
    auto& vertexList = ObjListStore::create_objlist<Vertex>();
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
    if (id == 0) {
        log_msg("Finish loading DIMACS graph.");
        log_msg("\tvertex num: "+to_string(vertexNum)+", edge num: "+to_string(edgeNum)
            +", src vertex: "+to_string(srcVertex)+", dst vertex: "+to_string(dstVertex)+", wall time: "+myTimer.format(4, "%w"));
    }
    myTimer.start();

    // Common variables
    auto& chV2V = ChannelStore::
        create_push_combined_channel<pair<int, vector<int>>, KeyMinCombiner<int, vector<int>>>(vertexList, vertexList);
    lib::Aggregator<int> visited, dstVisited;
    //lib::Aggregator<int> maxDist(0, [](int& a, const int& b) {if(b>a) a=b;});

    // Init
    list_execute(vertexList, [&](Vertex& v) {
        if (v.id()==srcVertex) {
            v.dist = 0;
            visited.update(1);
            for (auto a : v.resCaps) chV2V.push({v.dist+1, {v.id()}}, a.first); // use unit-length edge in finding shortest path
        }
    });
    lib::AggregatorFactory::sync();

    // Main Loop
    int iter=0;
    while (dstVisited.get_value() == 0) {
        list_execute(vertexList, [&](Vertex& v) {
            if (v.dist==-1 && chV2V.has_msgs(v)) {
                if (v.id() == dstVertex) {
                    auto& msg = chV2V.get(v);
                    v.dist = msg.first;
                    string path;
                    for (auto u : msg.second) path += to_string(u)+" -> ";
                    path += to_string(v.id());
                    log_msg("dst dist: "+to_string(v.dist));
                    log_msg("shortest path: "+path);
                    dstVisited.update(1); // ask to stop
                }
                else{
                    auto msg = chV2V.get(v);
                    v.dist = msg.first;
                    visited.update(1);
                    msg.first = v.dist+1;
                    msg.second.push_back(v.id());
                    for (auto a : v.resCaps) chV2V.push(msg, a.first);
                }
            }
        });
        lib::AggregatorFactory::sync();
        ++iter;
        if (id == 0) log_msg("iter: "+to_string(iter)+", visited: "+to_string(visited.get_value())
                +", wall time: "+myTimer.format(4, "%w"));
    }

    // Summary
    if (id == 0)
        log_msg("dist: "+to_string(iter)+", wall time: "+myTimer.format(4, "%w"));
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
        run_job(mf);
        log_msg("total wall time: "+myTimer.format(4, "%w"));
        return 0;
    }
    return 1;
}
