
include_directories( ${ENGINE_COMMON_INCLUDES} )

set(rebuildEM_SRCS main.cpp rebuildEM.cpp)
add_executable(rebuildEM ${rebuildEM_SRCS})
target_link_libraries(rebuildEM ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${MARIADB_CLIENT_LIBS})
install(TARGETS rebuildEM DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

if (WITH_REBUILD_EM_UT)
  set(rebuildEM_LIB_SRCS rebuildEM.cpp)
  add_library(rebuildEMTest SHARED ${rebuildEM_LIB_SRCS})
  target_link_libraries(rebuildEMTest ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${MARIADB_CLIENT_LIBS})
  install(FILES rebuildEM.h DESTINATION include)
  install(TARGETS rebuildEMTest DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
endif()
