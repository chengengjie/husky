#include <boost/config.hpp>
#include <iostream>
#include <string>
#include <ctime>
#include <boost/timer/timer.hpp>
#include <boost/graph/edmonds_karp_max_flow.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/read_dimacs.hpp>
#include <boost/graph/graph_utility.hpp>

// Use a DIMACS network flow file as stdin.
// edmonds-karp-eg < max_flow.dat

int
main()
{
  using namespace boost;

  typedef adjacency_list_traits < vecS, vecS, directedS > Traits;
  typedef adjacency_list < listS, vecS, directedS,
    property < vertex_name_t, std::string >,
    property < edge_capacity_t, long,
    property < edge_residual_capacity_t, long,
    property < edge_reverse_t, Traits::edge_descriptor > > > > Graph;

  Graph g;

  property_map < Graph, edge_capacity_t >::type
    capacity = get(edge_capacity, g);
  property_map < Graph, edge_reverse_t >::type rev = get(edge_reverse, g);
  property_map < Graph, edge_residual_capacity_t >::type
    residual_capacity = get(edge_residual_capacity, g);

  Traits::vertex_descriptor s, t;
  read_dimacs_max_flow(g, capacity, rev, s, t);

  timer::auto_cpu_timer myTimer;
  double time = clock();

  long flow = edmonds_karp_max_flow(g, s, t);

  std::cout << "total time is " << (clock()-time)/CLOCKS_PER_SEC << " second" << std::endl;

  //std::cout << "c  The total flow:" << std::endl;
  std::cout << "s " << flow << std::endl << std::endl;

  //std::cout << "c flow values:" << std::endl;
  //graph_traits < Graph >::vertex_iterator u_iter, u_end;
  //graph_traits < Graph >::out_edge_iterator ei, e_end;
  //for (boost::tie(u_iter, u_end) = vertices(g); u_iter != u_end; ++u_iter)
  //  for (boost::tie(ei, e_end) = out_edges(*u_iter, g); ei != e_end; ++ei)
  //    if (capacity[*ei] > 0)
  //      std::cout << "f " << *u_iter << " " << target(*ei, g) << " "
  //        << (capacity[*ei] - residual_capacity[*ei]) << std::endl;

  return EXIT_SUCCESS;
}
