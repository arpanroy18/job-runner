cmake_minimum_required(VERSION 3.16)
project(jr CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

add_executable(jr
  src/main.cpp
  src/common/net.cpp
  src/common/proto.cpp
  src/common/util.cpp
  src/sched/journal.cpp
  src/sched/scheduler.cpp
  src/worker/worker.cpp
  src/cli/cli.cpp
)

target_link_libraries(jr PRIVATE pthread)
