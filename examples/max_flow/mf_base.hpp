#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

#include "boost/tokenizer.hpp"
#include <boost/timer/timer.hpp>

using namespace std;
using namespace husky;
using base::log_msg;

class VertexBase {
public:
    using KeyT = int;

    VertexBase() = default;
    VertexBase(const KeyT& i) : vertexId(i) {}
    const KeyT& id() const { return vertexId; }

    // Serialization and deserialization
    friend BinStream& operator<<(BinStream& stream, const VertexBase& v) {
        stream << v.vertexId << v.caps;
        return stream;
    }
    friend BinStream& operator>>(BinStream& stream, VertexBase& v) {
        stream >> v.vertexId >> v.caps;
        return stream;
    }

    KeyT vertexId;
    vector<pair<KeyT, int>> caps;
};

class VertexEK : public VertexBase {
public:
    VertexEK(const KeyT& i) : VertexBase(i) {}
    unordered_map<KeyT, int> resCaps; // (id, edge weight)
    int dist; // -1 stands for "not visited"
};

class VertexEKPredecessor : public VertexBase {
public:
    VertexEKPredecessor(const KeyT& i) : VertexBase(i) {}
    unordered_map<KeyT, int> resCaps; // (id, edge weight)
    int dist; // -1 stands for "not visited"
    int pre;
};

class FlowMsgT {
public:
    FlowMsgT(int value, int height) : v(value), h(height) {}
    FlowMsgT() {}
    int v, h; // flow value, height
};

class FlowT {
public:
    FlowT(int cap, const FlowMsgT& fmsg) : c(cap), fm(fmsg) {}
    FlowT(int cap, int value, int height) : c(cap), fm(value, height) {}
    int c;
    FlowMsgT fm;
};

class VertexPR : public VertexBase {
public:
    VertexPR(const KeyT& i) : VertexBase(i) {}
    int height;
    int excess; // excess flow
    // for dense graph, unordered_map is better
    unordered_map<KeyT, FlowT> preflows; // (neight id, flow), flow.v > 0 means outgoing
};

template <typename K, typename V>
struct KeyMinCombiner {
    static void combine(pair<K, V>& val, pair<K, V> const& other) { if (other.first < val.first) val = other; }
};

inline bool print() { return Context::get_global_tid()==0; }

struct GraphStat{
    int vNum;
    int eNum;
    int srcV;
    int dstV;
};

template <typename VertexT>
void LoadDIMAXCSGraph(GraphStat& stat){
    // Read DIMAXCS file
    lib::Aggregator<int> vertexNumAgg, edgeNumAgg, srcVertexAgg, dstVertexAgg;
    auto& infmt = io::InputFormatStore::create_line_inputformat();
    infmt.set_input(Context::get_param("hdfs_input"));
    auto& vertexList = ObjListStore::create_objlist<VertexT>("vertexList");
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
            //chIn2V.push({u, c}, v);
            chIn2V.push({u, -1}, v);
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
    list_execute(vertexList, [&](VertexT& v) {
        auto& msgs = chIn2V.get(v);
        for (auto& m : msgs) {
            if (m.second != -1) v.caps.push_back(m);
        }
    });
    //globalize(vertexList);
    lib::AggregatorFactory::sync();
    stat.vNum = vertexNumAgg.get_value();
    stat.eNum = edgeNumAgg.get_value();
    stat.srcV = srcVertexAgg.get_value();
    stat.dstV = dstVertexAgg.get_value();

    if (print()) {
        log_msg("Finish loading DIMACS graph.");
        log_msg("\tvertex num: "+to_string(stat.vNum)
            +", edge num: "+to_string(stat.eNum)
            +", src vertex: "+to_string(stat.srcV)
            +", dst vertex: "+to_string(stat.dstV));
    }
}
