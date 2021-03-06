
include_directories( ${ENGINE_COMMON_INCLUDES} )

set(rebuildEM_SRCS main.cpp)
add_executable(rebuildEM ${rebuildEM_SRCS})
target_link_libraries(rebuildEM ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${MARIADB_CLIENT_LIBS})
install(TARGETS rebuildEM DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
