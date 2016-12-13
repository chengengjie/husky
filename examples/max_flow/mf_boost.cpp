#include <boost/config.hpp>
#include <iostream>
#include <string>
#include <ctime>
#include <boost/timer/timer.hpp>
#include <boost/graph/edmonds_karp_max_flow.hpp>
#include <boost/graph/push_relabel_max_flow.hpp>
#include <boost/graph/boykov_kolmogorov_max_flow.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/read_dimacs.hpp>
#include <boost/graph/graph_utility.hpp>

int main(int argc, char** argv)
{
    using namespace boost;
    using namespace std;

    string methods[3] = {"ek", "pr", "bk"};

    int mid = 0;
    if (argc > 1) {
        string method(argv[1]);
        for (int i=0; i<3; ++i)
            if (method == methods[i]){
                mid = i;
                break;
            }
    }
    cout << "Method is: " << methods[mid] << endl;

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
    timer::auto_cpu_timer myTimer;
    read_dimacs_max_flow(g, capacity, rev, s, t);
    cout << "Time for loading the graph is: " << endl;
    myTimer.report();

    myTimer.start();
    long flow = 0;
    if (mid == 0)
        flow = edmonds_karp_max_flow(g, s, t);
    else if (mid == 1)
        flow = push_relabel_max_flow(g, s, t);
    //else if (mid == 2)
        //flow = boykov_kolmogorov_max_flow(g, s, t);
    cout << "Max flow is: " << flow << endl;
    cout << "Time for computing max flow is: " << endl;
    myTimer.report();

    return EXIT_SUCCESS;
}
