#pragma once
#include "common/admin_socket_client.h"
#include <filesystem>
#include <string>
#include <map>
#include <vector>

namespace fs = std::filesystem;

class DaemonMetricCollector {
 public:
  int i;
  void main();

private:
  // TODO: add clients
  //       check removed sockets
  //       list dir of sockets
  std::map<std::string, AdminSocketClient> clients;
  fs::path socketdir = "/var/run/ceph";
  void update_sockets();
  void send_request_per_client();
  // void transform_perf_data();
  // int start_prometheus_server();
};
