include_directories(${ENGINE_COMMON_INCLUDES})

set(updateMeta_SRCS updateMeta.cpp)
add_executable(updateMeta ${updateMeta_SRCS})
target_link_libraries(updateMeta ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} fdbcs)
install(TARGETS updateMeta DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
