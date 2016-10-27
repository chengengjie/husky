#include "read.hpp"

void ReadDIMACS(std::vector<std::vector<int>>& Graph, int& source, std::istream& in){
    char flag;
    while(in.get(flag)){
        if(flag == '\n'){
            continue;
        }else if(flag == 'c'){
            in.ignore(std::numeric_limits<std::streamsize>::max(), '\n');    
        }else if(flag == 'p'){
            static bool lock=false;
            assert(!lock);
            lock = true;
            std::string t;
            in >> t;
            assert(t=="max");
            int n, m;
            in >> n >> m;
            std::cout << "number of nodes is " << n << "; number of edges is " << m << std::endl;
            Graph.resize(n);
        }else if(flag == 'a'){
            int u, v, c;
            in >> u >> v >> c;
            --u; --v;
            assert(u<Graph.size() && v<Graph.size());
            Graph[u].push_back(v);
        }else if(flag == 'n'){
            int s;
            char st;
            in >> s >> st;
            if (st == 's') source = s-1;
        }else std::cout << "unknown input flag: " << flag << std::endl;
    }
}
