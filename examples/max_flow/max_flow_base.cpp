#include "max_flow_base.hpp"

void LoadDIMAXCSGraph(int& srcVertex, int& dstVertex){
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
    int vertexNum=vertexNumAgg.get_value();
    int edgeNum=edgeNumAgg.get_value();
    srcVertex=srcVertexAgg.get_value();
    dstVertex=dstVertexAgg.get_value();

    if (print()) {
        log_msg("Finish loading DIMACS graph.");
        log_msg("\tvertex num: "+to_string(vertexNum)+", edge num: "+to_string(edgeNum)
            +", src vertex: "+to_string(srcVertex)+", dst vertex: "+to_string(dstVertex));
    }
}
