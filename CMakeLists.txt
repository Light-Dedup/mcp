cmake_minimum_required(VERSION 3.2)
project(main)

IF (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING
        "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
ENDIF()
add_compile_options(-Wall -Wextra -fexceptions)
IF (CMAKE_BUILD_TYPE STREQUAL Debug)
    ADD_DEFINITIONS(-DDEBUG)
ENDIF()

aux_source_directory(src SRCS)
add_executable(${PROJECT_NAME} ${SRCS})
target_include_directories(${PROJECT_NAME}
	PRIVATE
		${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(${PROJECT_NAME}
	PRIVATE
        pthread
)
#target_compile_features(${PROJECT_NAME} PRIVATE cxx_auto_type)
set(CMAKE_CXX_STANDARD 11)

#target_compile_features(${PROJECT_NAME} PRIVATE c_auto_type)
set(CMAKE_C_STANDARD 11)

