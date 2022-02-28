#include "DaemonMetricCollector.h"
#include "common/admin_socket_client.h"

#include <iostream>
#include <string>
#include <filesystem>

void DaemonMetricCollector::main() {
  std::cout << "metric" << std::endl;
  send_request_per_client();
  // start server
  while (1) {
    // request_perfcounters();
    // 1. updates sockets
    // 1.1 list dir
    // 1.2 updates vector
    // 2. send request per client
    // 3. update global perf counters
    update_sockets();
   }
}

void update_sockets() {
  for (const auto &entry : fs::recursive_directory_iterator(socketdir)) {
    if (entry.path().extension() == ".asok") {
      if (clients.find(entry.path()) == clients.end()) {
      AdminSocketClient sock(entry.path());
      clients[entry.path()] = sock;
    }
    }
  }
}

void DaemonMetricCollector::send_request_per_client() {
  AdminSocketClient mgr_sock_client("/var/run/ceph/whatever");
  std::string request("{\"prefix\":\"perf dump\"}");
  std::string response;
  mgr_sock_client.do_request(request, &response);
  std::cout << response << std::endl;
}
