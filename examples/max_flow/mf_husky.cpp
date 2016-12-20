#include "mf_base.hpp"

void EdmondsKarpPredecessor();
void EdmondsKarpPrefix();
void PushRelabel();

int main(int argc, char** argv)
{
    boost::timer::cpu_timer myTimer;
    vector<string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("hdfs_input");
    args.push_back("print");
    if (init_with_args(argc, argv, args)) {
        myTimer.start();
        log_msg("Go to Edmonds Karp - prefix");
        //run_job(EdmondsKarpPrefix);
        log_msg("total wall time: "+myTimer.format(4, "%w")+"\n");

        myTimer.start();
        log_msg("Go to Edmonds Karp - predecessor");
        run_job(EdmondsKarpPredecessor);
        log_msg("total wall time: "+myTimer.format(4, "%w")+"\n");

        myTimer.start();
        log_msg("Go to push relabel");
        //run_job(PushRelabel);
        log_msg("total wall time: "+myTimer.format(4, "%w")+"\n");
        return 0;
    }
    return 1;
}
