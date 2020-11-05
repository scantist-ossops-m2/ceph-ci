function(build_uring)
  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  include(ExternalProject)
  ExternalProject_Add(liburing_ext
    GIT_REPOSITORY https://git.kernel.dk/liburing
    GIT_TAG "liburing-0.7"
    SOURCE_DIR ${CMAKE_BINARY_DIR}/src/liburing
    CONFIGURE_COMMAND <SOURCE_DIR>/configure
    BUILD_COMMAND env CC=${CMAKE_C_COMPILER} ${make_cmd} -C src -s
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS "<SOURCE_DIR>/src/liburing.a"
    INSTALL_COMMAND "")
  unset(make_cmd)

  ExternalProject_Get_Property(liburing_ext source_dir)
  set(URING_INCLUDE_DIR "${source_dir}/src/include")
  set(URING_LIBRARY_DIR "${source_dir}/src")

  add_library(uring::uring STATIC IMPORTED GLOBAL)
  add_dependencies(uring::uring liburing_ext)
  file(MAKE_DIRECTORY ${URING_INCLUDE_DIR})
  set_target_properties(uring::uring PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${URING_INCLUDE_DIR}
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${URING_LIBRARY_DIR}/liburing.a")
endfunction()
