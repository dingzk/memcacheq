#CMake最低版本号要求
cmake_minimum_required(VERSION 3.15)

#项目信息,工程顶层目录名称
project(memcacheq)


#使用C++ 11标准
#set( CMAKE_CXX_COMPILER  "g++ -std=c++11" )
set(CMAKE_CXX_STANDARD 11)

# C99 标准
set(CMAKE_C_STANDARD 99)

# 启用调试/发版模式
#SET(CMAKE_BUILD_TYPE Release)
SET(CMAKE_BUILD_TYPE Debug)
# 显示编译过程
set(CMAKE_VERBOSE_MAKEFILE ON)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")


add_subdirectory(src)
add_subdirectory(tests)

