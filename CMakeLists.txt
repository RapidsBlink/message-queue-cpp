cmake_minimum_required(VERSION 3.6)
project(queue_race)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD 11)
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH}) # To allow CMake to locate our Find*.cmake files
set(CMAKE_BUILD_TYPE "Release")
# Options. Turn on with 'cmake -Dmyvarname=ON'.
option(test "Build all tests." OFF) # Makes boolean 'test' available.
option(enableLOG "enable loging" OFF)
set_property(GLOBAL PROPERTY RULE_MESSAGES OFF)
option(enableWall "enable -Wall" OFF)

add_compile_options(-O3)

if(enableWall)
add_compile_options(-Wall -g)
endif()

if (enableLOG)
    add_definitions(-DUSE_LOG)
endif ()
include(GetGitRevisionDescription)
get_git_head_revision(GIT_REFSPEC GIT_SHA1)
add_definitions("-DGIT_SHA1=\"${GIT_SHA1}\"")

include(FindSSE)
if (AVX2_FOUND)
    #message("found AVX2")
    add_compile_options(-mavx2)
elseif (AVX_FOUND)
    message("found AVX")
    add_compile_options(-mavx)
elseif (SSE4_1_FOUND)
    message("found sse4.1")
    add_compile_options(-msse4.1)
elseif (SSE2_FOUND)
    message("found sse2")
    add_compile_options(-msse2)
endif ()

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-mpopcnt" COMPILER_OPT_POPCNT_SUPPORTED)
if (COMPILER_OPT_POPCNT_SUPPORTED)
    add_compile_options(-mpopcnt)
endif ()

find_package(Threads)
find_package(Tcmalloc)
if (Tcmalloc_FOUND)
    include_directories(${Tcmalloc_INCLUDE_DIR})
    set(TCMALLOC_LIBS ${Tcmalloc_LIBRARIES})
endif()

message("-- Found Snappy: TRUE")

#find_package(TBB REQUIRED)
#include_directories(${TBB_INCLUDE_DIRS})

if (AVX2_FOUND)
    set(BASE64_AVX2_FILES_LEMIRE src/dependencies/fast_base64/fastavxbase64.c src/dependencies/fast_base64/fastavxbase64.h)
endif ()

set(CPP_SRC
        include/queue_store.h
        src/utils/utils.cpp src/utils/utils.h
        src/utils/log.c src/utils/log.h
        src/io/asyncfileio.hpp src/io/blockingQueue.hpp
        src/utils/serialization.h src/utils/serialization.cpp
        src/dependencies/fast_base64/chromiumbase64.c ${BASE64_AVX2_FILES_LEMIRE} include/queue_store_config.h src/io/Barrier.hpp src/io/ConcurrentSet.hpp)

add_library(queue_race SHARED include/queue_store.h src/queue_store.cpp ${CPP_SRC})
target_link_libraries(queue_race ${CMAKE_THREAD_LIBS_INIT} ${TCMALLOC_LIBS})

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/
        DESTINATION include)
install(TARGETS queue_race
        LIBRARY DESTINATION lib)
