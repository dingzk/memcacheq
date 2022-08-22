include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/dep/libevent/include)
link_directories(${PROJECT_SOURCE_DIR}/dep/libevent/lib)
include_directories(${PROJECT_SOURCE_DIR}/dep/jemalloc/include)
link_directories(${PROJECT_SOURCE_DIR}/dep/jemalloc/lib)

set(LIBJIEMALLOC_TARGET "${PROJECT_SOURCE_DIR}/dep/jemalloc/lib/libjemalloc.a")

add_custom_command(
        OUTPUT ${LIBJIEMALLOC_TARGET}
        COMMAND
        sh autogen.sh
        COMMAND
        ./configure
        COMMAND
        make -j 16
        WORKING_DIRECTORY
        ${PROJECT_SOURCE_DIR}/dep/jemalloc
        COMMENT
        "building libjemalloc.a"
)

#add_custom_target(libjemalloc ALL
add_custom_target(libjemalloc
        DEPENDS
        ${LIBJIEMALLOC_TARGET}
        )

AUX_SOURCE_DIRECTORY(${PROJECT_SOURCE_DIR}/src ROOT_SRC)

add_executable(memcacheq ${ROOT_SRC})
add_dependencies(memcacheq libjemalloc)

target_link_libraries(memcacheq libjemalloc.a libevent.a pthread m dl)
