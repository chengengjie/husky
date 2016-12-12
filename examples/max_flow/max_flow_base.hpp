#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

#include "boost/tokenizer.hpp"
#include <boost/timer/timer.hpp>

using namespace std;
using namespace husky;
using base::log_msg;

class Vertex {
public:
    using KeyT = int;

    Vertex() = default;
    explicit Vertex(const KeyT& i) : vertexId(i) {}
    const KeyT& id() const { return vertexId; }

    // Serialization and deserialization
    friend BinStream& operator<<(BinStream& stream, const Vertex& v) {
        stream << v.vertexId << v.caps << v.resCaps << v.dist << v.pre;
        return stream;
    }
    friend BinStream& operator>>(BinStream& stream, Vertex& v) {
        stream >> v.vertexId >> v.caps >> v.resCaps >> v.dist >> v.pre;
        return stream;
    }

    KeyT vertexId;
    vector<pair<KeyT, int>> caps;
    unordered_map<KeyT, int> resCaps; // (id, edge weight)
    int pre;
    int dist; // -1 stands for "not visited"
};

template <typename K, typename V>
struct KeyMinCombiner {
    static void combine(pair<K, V>& val, pair<K, V> const& other) { if (other.first < val.first) val = other; }
};

inline bool print() { return Context::get_global_tid()==0; }

void LoadDIMAXCSGraph(int& srcVertex, int& dstVertex);
