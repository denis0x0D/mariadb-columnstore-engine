
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(rebuildEM_SRCS main.cpp)

add_executable(rebuildEM ${rebuildEM_SRCS})

target_link_libraries(rebuildEM ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS rebuildEM DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

