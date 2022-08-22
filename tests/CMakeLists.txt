include_directories(${PROJECT_SOURCE_DIR}/src)

file(GLOB TEST_QUEUE_SRC testq.c ../src/pq_manager.c ../src/pqueue.c ../src/murmur3_hash.c ../src/hash.c ../src/hashtable.c)

file(GLOB TEST_ATOMIC_SRC atomic.c)

add_executable(testq  ${TEST_QUEUE_SRC})
target_link_libraries(testq pthread m)

add_executable(test_atomic  ${TEST_ATOMIC_SRC})
target_link_libraries(test_atomic pthread)

