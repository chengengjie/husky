#include "read.hpp"

#include "core/engine.hpp"
#include "io/input/hdfs_line_inputformat.hpp"
#include "lib/aggregator_factory.hpp"

#include <boost/timer/timer.hpp>

class Vertex {
public:
    using KeyT = int;

    Vertex() = default;
    explicit Vertex(const KeyT& id) : vertex_id(id) {}
    const KeyT& id() const { return vertex_id; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& v) {
        stream << v.vertex_id << v.adj << v.dist;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& v) {
        stream >> v.vertex_id >> v.adj >> v.dist;
        return stream;
    }

    KeyT vertex_id;
    std::vector<KeyT> adj;
    int dist = -1; // -1 stands for "not visited"
};

void cc() {
    // Create and globalize vertex objects
    auto& vertex_list = husky::ObjListFactory::create_objlist<Vertex>();
    int vertex_num;
    husky::lib::Aggregator<int> total(0, [](int& a, const int& b) { a += b; });
    auto& agg_ch = husky::lib::AggregatorFactory::get_channel();

    if (husky::Context::get_global_tid() == 0){
        auto fn = husky::Context::get_param("input");
        husky::base::log_msg("input DIMACS file is "+fn);
        std::ifstream ifs(fn);
        std::vector<std::vector<int>> g;
        int s;
        ReadDIMACS(g, s, ifs);
        husky::base::log_msg("finish reading "+fn);
        vertex_num = g.size();
        total.update(vertex_num);
        for (int i=0; i<vertex_num; ++i){
            Vertex v(i);
            for (auto a:g[i]) v.adj.push_back(a);
            if (i==s) v.dist = 0;
            vertex_list.add_object(std::move(v));
        }
    }
    agg_ch.out();
    vertex_num = total.get_value();
    husky::globalize(vertex_list);

    auto& ch =
        husky::ChannelFactory::create_push_combined_channel<int, husky::MinCombiner<int>>(vertex_list, vertex_list);

    husky::lib::Aggregator<int> visited(0, [](int& a, const int& b) { a += b; });
    visited.to_keep_aggregate();

    husky::list_execute(vertex_list, {&ch}, {&ch, &agg_ch}, [&ch, &visited](Vertex& v) {
        if (v.dist==0) {
            visited.update(1);
            for (auto a : v.adj) ch.push(v.dist, a);
        }
    });

    // Main Loop
    int dist=0;
    while (visited.get_value() < vertex_num) {
        ++dist;
        husky::list_execute(vertex_list, {&ch}, {&ch, &agg_ch}, [&ch, &visited](Vertex& v) {
            if (v.dist==-1 && ch.has_msgs(v)) {
                v.dist = ch.get(v)+1;
                visited.update(1);
                for (auto a : v.adj) ch.push(v.dist, a);
            }
        });
    }

    if (husky::Context::get_global_tid() == 0) husky::base::log_msg("dist: "+std::to_string(dist));
    std::string small_graph = husky::Context::get_param("print");
    if (small_graph == "1") {
        husky::list_execute(vertex_list, [](Vertex& v) {
            husky::base::log_msg("vertex: "+std::to_string(v.id()) + " dist: "+std::to_string(v.dist));
        });
    }
}

int main(int argc, char** argv)
{
    boost::timer::auto_cpu_timer myTimer;
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("print");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(cc);
        return 0;
    }
    return 1;
}
