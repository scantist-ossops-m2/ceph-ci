#include "common/ceph_argparse.h"
#include "common/config.h"
#include "exporter/DaemonMetricCollector.h"
#include "exporter/http_server.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include <boost/thread/thread.hpp>
#include <iostream>
#include <map>
#include <string>

#define dout_context g_ceph_context

static void usage() {
  std::cout << "usage: ceph-exporter [--sock-path=<sock_path>] [--addrs=<addrs>] [--port=<port>]/n"
            << "--sock-path: The path to ceph daemons socket files\n"
            << "--addrs: Host ip address where exporter is deployed\n"
            << "--port: Port to deploy exporter on. Default is 9926"
            << std::endl;
  generic_client_usage();
}

int main(int argc, char **argv) {

  auto args = argv_to_vec(argc, argv);
  // CephContext *cct_;
  if (args.empty()) {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_DAEMON, 0);
  std::string val, sock_path, exporter_addrs, exporter_port;
  for (auto i = args.begin(); i != args.end();) {
    if (ceph_argparse_witharg(args, i, &val, "--sock-path", (char *)NULL)) {
      sock_path = val;
      cct->_conf.set_val("sock_path", val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--addrs", (char *)NULL)) {
      exporter_addrs = val;
      cct->_conf.set_val("exporter_addrs", val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--port", (char *)NULL)) {
      exporter_port = val;
      cct->_conf.set_val("exporter_port", val);
    }
    else if (ceph_argparse_flag(args, i, "--help", (char *)NULL) ||
               ceph_argparse_flag(args, i, "-h", (char *)NULL)) {
      usage();
      exit(0);
    }
  }
  common_init_finish(g_ceph_context);

  boost::thread server_thread(http_server_thread_entrypoint, exporter_addrs, exporter_port);
  DaemonMetricCollector &collector = collector_instance();
  collector.set_sock_path(sock_path);
  collector.main();
  server_thread.join();
}
