#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

#include "boost/tokenizer.hpp"
#include <boost/timer/timer.hpp>

class Vertex {
public:
    using KeyT = int;

    Vertex() = default;
    explicit Vertex(const KeyT& i) : vertexId(i) {}
    const KeyT& id() const { return vertexId; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& v) {
        stream << v.vertexId << v.adjs << v.dist;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& v) {
        stream >> v.vertexId >> v.adjs >> v.dist;
        return stream;
    }

    KeyT vertexId;
    std::vector<KeyT> adjs;
    int dist = -1; // -1 stands for "not visited"
};

void mf() {
    boost::timer::cpu_timer myTimer;
    if (husky::Context::get_global_tid() == 0)
        husky::base::log_msg("Start.");

    // Load vertex list
    // local read
    auto& infmt = husky::io::InputFormatStore::create_line_inputformat();
    infmt.set_input(husky::Context::get_param("hdfs_input"));
    int vertexNum, edgeNum, srcVertex;
    husky::lib::Aggregator<int> vertexNumAgg(0, [](int& a, const int& b) { a = b; });
    husky::lib::Aggregator<int> edgeNumAgg(0, [](int& a, const int& b) { a += b; });
    husky::lib::Aggregator<int> srcVertexAgg(0, [](int& a, const int& b) { a = b; });
    auto& vertexList = husky::ObjListStore::create_objlist<Vertex>();
    auto& inputToVertexCh = husky::ChannelStore::create_push_channel<int>(infmt, vertexList);
    auto parseDIMACS = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0) return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        char flag = it->at(0);
        //husky::base::log_msg(std::string(1, flag));
        ++it;
        if(flag == 'c' || flag == '\n'){
            return;
        }else if(flag == 'p'){
            //assert(*it=="max");
            ++it;
            int n = stoi(*it++), m = stoi(*it++);
            vertexNumAgg.update(n);
            edgeNumAgg.update(m);
        }else if(flag == 'a'){
            int u = stoi(*it++)-1, v = stoi(*it++)-1, c = stoi(*it++);
            inputToVertexCh.push(u,v);
            inputToVertexCh.push(v,u);
        }else if(flag == 'n'){
            int s = stoi(*it++)-1;
            char st = it->at(0);
            if (st == 's') srcVertexAgg.update(s);
        }else std::cout << "unknown input flag: " << flag << std::endl;
    };
    husky::load(infmt, parseDIMACS);
    // receive
    husky::list_execute(vertexList, [&](Vertex& v) {
        auto adjs = inputToVertexCh.get(v);
        v.adjs = adjs;
    });
    husky::globalize(vertexList);
    husky::lib::AggregatorFactory::sync();
    vertexNum   = vertexNumAgg.get_value();
    edgeNum     = edgeNumAgg.get_value();
    srcVertex   = srcVertexAgg.get_value();
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Finish loading DIMACS graph.");
        husky::base::log_msg("\tvertex number: "+std::to_string(vertexNum)+", edge number: "+std::to_string(edgeNum)
            +", source vertex: "+std::to_string(srcVertex)+", wall time: "+myTimer.format(4, "%w"));
    }
    myTimer.resume();

    // Common data
    auto& ch =
        husky::ChannelStore::create_push_combined_channel<int, husky::MinCombiner<int>>(vertexList, vertexList);
    husky::lib::Aggregator<int> visited(0, [](int& a, const int& b) { a += b; });
    visited.to_keep_aggregate();

    // Init
    husky::list_execute(vertexList, [&](Vertex& v) {
        if (v.id()==srcVertex) {
            v.dist = 0;
            visited.update(1);
            for (auto a : v.adjs) ch.push(v.dist, a);
        }
    });

    // Main Loop
    int dist=0;
    while (visited.get_value() < vertexNum) {
        ++dist;
        if (husky::Context::get_global_tid() == 0)
            husky::base::log_msg("iter: "+std::to_string(dist)+", visited: "
                +std::to_string(visited.get_value())+", wall time: "+myTimer.format(4, "%w"));
        husky::list_execute(vertexList, [&](Vertex& v) {
            if (v.dist==-1 && ch.has_msgs(v)) {
                v.dist = ch.get(v)+1;
                visited.update(1);
                for (auto a : v.adjs) ch.push(v.dist, a);
            }
        });
        husky::lib::AggregatorFactory::sync();
    }

    // Summary
    if (husky::Context::get_global_tid() == 0)
        husky::base::log_msg("dist: "+std::to_string(dist)+", wall time: "+myTimer.format(4, "%w"));
    std::string small_graph = husky::Context::get_param("print");
    if (small_graph == "1") {
        husky::list_execute(vertexList, [](Vertex& v) {
            husky::base::log_msg("vertex: "+std::to_string(v.id()) + " dist: "+std::to_string(v.dist));
        });
    }
}

int main(int argc, char** argv)
{
    boost::timer::cpu_timer myTimer;
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("hdfs_input");
    args.push_back("print");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(mf);
        husky::base::log_msg("total wall time: "+myTimer.format(4, "%w"));
        return 0;
    }
    return 1;
}
