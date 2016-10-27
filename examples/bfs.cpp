#include <boost/config.hpp>
#include <iostream>
#include <string>
#include <boost/timer/timer.hpp>
#include <boost/graph/edmonds_karp_max_flow.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/read_dimacs.hpp>
#include <boost/graph/graph_utility.hpp>
#include <boost/graph/visitors.hpp>
#include <boost/graph/breadth_first_search.hpp>

/*
#include "core/engine.hpp"
#include "io/input/hdfs_line_inputformat.hpp"
#include "lib/aggregator_factory.hpp"
*/

// Use a DIMACS network flow file as stdin.
// edmonds-karp-eg < max_flow.dat

using namespace boost;

// time_stamper

// distance recorder 2 (distance recorder + max dist)
template <class DistMap, class Dist, class Tag>
struct distance_recorder2 : public base_visitor<distance_recorder2<DistMap, Dist, Tag>>
{
    typedef Tag event_filter;
    distance_recorder2(DistMap dm, Dist& d) : distMap(dm), maxDist(d) {}

    template <class Edge, class Graph>
    void operator()(Edge e, const Graph& g)
    {
        auto dist = get(distMap, source(e, g))+1;
        put(distMap, target(e, g), dist);
        if (dist > maxDist) maxDist = dist;
    }

    DistMap distMap;
    Dist& maxDist;
};
template <class DistMap, class Dist, class Tag>
distance_recorder2<DistMap, Dist, Tag> 
make_distance_recorder2(DistMap dm, Dist& d, Tag) {
    return distance_recorder2<DistMap, Dist, Tag>(dm, d);
}
/*
class Vertex {
public:
    using KeyT = int;
    enum Color { white, grey, black };

    Vertex() = default;
    explicit Vertex(const KeyT& id) : vertex_id(id), cid(id) {}
    const KeyT& id() const { return vertex_id; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& v) {
        stream << v.vertex_id << v.adj << v.cid << v.color;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& v) {
        stream >> v.vertex_id >> v.adj >> v.cid << v.color;
        return stream;
    }

    KeyT vertex_id, cid;
    std::vector<KeyT> adj;
    Color color = white;
};

void cc() {
    husky::io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    // Create and globalize vertex objects
    auto& vertex_list = husky::ObjListFactory::create_objlist<Vertex>();
    if (husky::Context::get_global_tid() == 0){
        typedef adjacency_list_traits<vecS, vecS, directedS> Traits;
        typedef adjacency_list<listS, vecS, directedS,
            property<vertex_name_t, std::string>,
            property<edge_capacity_t, long,
            property<edge_residual_capacity_t, long,
            property<edge_reverse_t, Traits::edge_descriptor>>> > Graph;
        Graph g;

        auto capacity = get(edge_capacity, g);
        auto rev = get(edge_reverse, g);
        auto vid = get(vertex_index, g);
        Traits::vertex_descriptor s, t;
        read_dimacs_max_flow(g, capacity, rev, s, t);

        for (auto vp = vertices(g); vp.first != vp.second; ++vp.first){
            auto v = *vp.first;
            Vertex hv(get(vid, g, v);
            if (v == s) hv.color = Vertex::grey;
            for (auto ep = out_edges(v, g); ep.first != ep.second; ++ep.first){
                auto ov = target(*ep.first, g);
                hv.adj.push_back(get(vid, g, ov));
            }
            vertex_list.add_object(std::move(hv));
        }

    }
    husky::globalize(vertex_list);

    auto& ch =
        husky::ChannelFactory::create_push_combined_channel<int, husky::MinCombiner<int>>(vertex_list, vertex_list);
    // Aggregator to check how many vertexes updating
    husky::lib::Aggregator<int> not_finished(0, [](int& a, const int& b) { a += b; });
    not_finished.to_reset_each_iter();
    not_finished.update(1);

    auto& agg_ch = husky::lib::AggregatorFactory::get_channel();

    // Initialization
    husky::list_execute(vertex_list, {}, {&ch, &agg_ch}, [&ch, &not_finished](Vertex& v) {
        // Get the smallest component id among neighbors
        for (auto nb : v.adj) {
            if (nb < v.cid)
                v.cid = nb;
        }
        // Broadcast my component id
        for (auto nb : v.adj) {
            if (nb > v.cid)
                ch.push(v.cid, nb);
        }
    });
    // Main Loop
    while (not_finished.get_value()) {
        if (husky::Context::get_global_tid() == 0)
            husky::base::log_msg("# updated in this round: "+std::to_string(not_finished.get_value()));
        husky::list_execute(vertex_list, {&ch}, {&ch, &agg_ch}, [&ch, &not_finished](Vertex& v) {
            if (ch.has_msgs(v)) {
                auto msg = ch.get(v);
                if (msg < v.cid) {
                    v.cid = msg;
                    not_finished.update(1);
                    for (auto nb : v.adj)
                        ch.push(v.cid, nb);
                }
            }
        });
    }
    std::string small_graph = husky::Context::get_param("print");
    if (small_graph == "1") {
        husky::list_execute(vertex_list, [](Vertex& v) {
            husky::base::log_msg("vertex: "+std::to_string(v.id()) + " component id: "+std::to_string(v.cid));
        });
    }
}
*/
int main(int argc, char** argv)
{
    timer::cpu_timer myTimer;

    // Graph
    typedef adjacency_list_traits<vecS, vecS, directedS> Traits;
    typedef adjacency_list<listS, vecS, directedS,
        property<vertex_name_t, std::string>,
        property<edge_capacity_t, long,
        property<edge_residual_capacity_t, long,
        property<edge_reverse_t, Traits::edge_descriptor>>> > Graph;

    Graph g;

    auto capacity = get(edge_capacity, g);
    auto residual_capacity = get(edge_residual_capacity, g);
    auto rev = get(edge_reverse, g);
    auto vid = get(vertex_index, g);
    Traits::vertex_descriptor s, t;
    typedef graph_traits<Graph>::vertices_size_type Size;
    read_dimacs_max_flow(g, capacity, rev, s, t);

    // Max flow
    /*myTimer.start();
    long flow = edmonds_karp_max_flow(g, s, t);
    std::cout << "max flow is " << flow << std::endl;
    std::cout << timer::format(myTimer.elapsed()) << std::endl;*/
   
    // BFS2
    std::vector<Size> dist2(num_vertices(g));
    myTimer.start();
    breadth_first_search(g, s, visitor(make_bfs_visitor(
        record_distances(make_iterator_property_map(dist2.begin(), vid), on_tree_edge()))));
    auto maxDist2 = *max_element(dist2.begin(), dist2.end());
    std::cout << "max dist is " << maxDist2 << std::endl;
    std::cout << timer::format(myTimer.elapsed()) << std::endl;;
    
    // BFS3
    std::vector<Size> dist3(num_vertices(g));
    auto dist3_pm = make_iterator_property_map(dist3.begin(), vid);
    Size maxDist3;
    myTimer.start();
    breadth_first_search(g, s, visitor(make_bfs_visitor(
        make_distance_recorder2(make_iterator_property_map(dist3.begin(), vid), maxDist3, on_tree_edge()))));
    std::cout << "max dist is " << maxDist3 << std::endl;
    std::cout << timer::format(myTimer.elapsed()) << std::endl;;
/*    
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
*/
    return EXIT_SUCCESS;
}
