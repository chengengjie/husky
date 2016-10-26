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

// Use a DIMACS network flow file as stdin.
// edmonds-karp-eg < max_flow.dat

using namespace boost;

template<typename TimeMap, typename DistMap>
class bfs_time_dist_visitor : public default_bfs_visitor
{
    typedef typename property_traits<TimeMap>::value_type T;
    typedef typename property_traits<DistMap>::value_type D;

public:
    bfs_time_dist_visitor(TimeMap tmap, DistMap dmap, T& t, D& d) 
        : m_timemap(tmap), m_distmap(dmap), m_time(t), m_dist(d) {}

    template<typename Vertex, typename Graph>
    void discover_vertex(Vertex u, const Graph& g) const
    {
        put(m_timemap, u, m_time++);
    }

    template<typename Edge, typename Graph>
    void tree_edge(Edge e, const Graph& g) const
    {
        auto dist = get(m_distmap, source(e, g))+1;
        put(m_distmap, target(e, g), dist);
        if (dist > m_dist) m_dist = dist;
    }

    TimeMap m_timemap;
    DistMap m_distmap;
    T& m_time;
    D& m_dist;
};

int main()
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
    read_dimacs_max_flow(g, capacity, rev, s, t);

    // Max flow
    /*myTimer.start();
    long flow = edmonds_karp_max_flow(g, s, t);
    std::cout << "max flow is " << flow << std::endl;
    std::cout << timer::format(myTimer.elapsed()) << std::endl;*/

    // BFS1
    typedef graph_traits<Graph>::vertices_size_type Size;
    std::vector<Size> dtime(num_vertices(g));
    std::vector<Size> dist(num_vertices(g));
    typedef iterator_property_map<std::vector<Size>::iterator,
        property_map<Graph, vertex_index_t>::const_type> dtime_pm_type;
    dtime_pm_type dtime_pm(dtime.begin(), vid);
    dtime_pm_type dist_pm(dist.begin(), vid);
    put(dist_pm, s, 0);
    Size time=0;
    Size maxDist=0;
    bfs_time_dist_visitor<dtime_pm_type, dtime_pm_type> vis(dtime_pm, dist_pm, time, maxDist);

    myTimer.start();
    breadth_first_search(g, s, visitor(vis));
    std::cout << "max dist is " << maxDist << std::endl;
    std::cout << timer::format(myTimer.elapsed()) << std::endl;;
    
    // BFS2
    std::vector<Size> dist2(num_vertices(g));
    dtime_pm_type dist2_pm(dist2.begin(), vid);
    myTimer.start();
    breadth_first_search(g, s, visitor(make_bfs_visitor(record_distances(dist2_pm, on_tree_edge()))));
    Size maxDist2=0;
    for (auto d:dist2) if (d>maxDist2) maxDist2=d;
    std::cout << "max dist is " << maxDist2 << std::endl;
    std::cout << timer::format(myTimer.elapsed()) << std::endl;;

    /*for (auto vp = vertices(g); vp.first!=vp.second; ++vp.first){
        auto v = *vp.first;
        std::cout << "vertex " << get(vid, v) << ": dist=" << get(dist_pm, v) 
            << "; time=" << get(dtime_pm, v) << std::endl;
    }*/

    return EXIT_SUCCESS;
}
