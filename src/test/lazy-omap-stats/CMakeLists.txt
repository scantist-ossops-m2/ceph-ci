# Lazy omap stat collection tests

if (POLICY CMP0144)
  cmake_policy(SET CMP0144 NEW)
find_package(Boost
             COMPONENTS regex
             REQUIRED)

add_executable(ceph_test_lazy_omap_stats
  main.cc
  lazy_omap_stats_test.cc)
target_link_libraries(ceph_test_lazy_omap_stats
  librados Boost::system Boost::regex ceph-common ${UNITTEST_LIBS})
install(TARGETS
  ceph_test_lazy_omap_stats
  DESTINATION ${CMAKE_INSTALL_BINDIR})
