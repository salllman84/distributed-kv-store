#include "store.hpp"
#include <iostream>
#include <cassert>

int main() {
    kvstore::Store db;
    std::cout << "--- Starting Storage Engine Tests ---\n";

    // 1. Test SET and GET
    db.set("node_id", "node_1");
    auto val1 = db.get("node_id");
    assert(val1.has_value() && val1.value() == "node_1");
    std::cout << "[SUCCESS] SET & GET\n";

    // 2. Test UPDATE
    db.set("node_id", "node_primary");
    assert(db.get("node_id").value() == "node_primary");
    std::cout << "[SUCCESS] UPDATE\n";

    // 3. Test DELETE
    bool is_deleted = db.remove("node_id");
    assert(is_deleted == true);
    assert(!db.get("node_id").has_value());
    std::cout << "[SUCCESS] DELETE\n";

    // 4. Test Missing Key
    assert(!db.get("missing_key").has_value());
    std::cout << "[SUCCESS] Missing Key Handling\n";

    std::cout << "--- All Tests Passed! Engine is ready. ---\n";
    return 0;
}