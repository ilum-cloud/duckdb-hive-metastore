# Find Thrift - vcpkg's ThriftConfig.cmake is broken, so use manual find This
# works for both vcpkg and system installations
find_path(THRIFT_INCLUDE_DIR NAMES thrift/Thrift.h REQUIRED)
# On Windows/vcpkg with MSVC, static libraries may have 'lib' prefix
find_library(THRIFT_LIBRARY NAMES libthrift thrift thriftmd libthriftmd
                                  REQUIRED)

if(NOT THRIFT_INCLUDE_DIR OR NOT THRIFT_LIBRARY)
  message(
    FATAL_ERROR
      "Thrift library not found. Install via: VCPKG_TOOLCHAIN_PATH=\$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake make"
  )
endif()

message(STATUS "Found Thrift: ${THRIFT_LIBRARY}")

# Create imported target for consistency (GLOBAL so it's visible to parent
# scope)
if(NOT TARGET thrift::thrift)
  add_library(thrift::thrift UNKNOWN IMPORTED GLOBAL)
  set_target_properties(
    thrift::thrift
    PROPERTIES IMPORTED_LOCATION "${THRIFT_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${THRIFT_INCLUDE_DIR}")
endif()

# Find the thrift compiler - check vcpkg tools first
find_program(
  THRIFT_COMPILER
  NAMES thrift
  PATHS
    "${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/tools/thrift"
    "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/tools/thrift"
  NO_DEFAULT_PATH)

# If not found in vcpkg tools, try system path
if(NOT THRIFT_COMPILER)
  find_program(THRIFT_COMPILER thrift)
endif()

if(NOT THRIFT_COMPILER)
  message(
    FATAL_ERROR "Thrift compiler not found. Please install thrift-compiler.")
endif()

set(THRIFT_FILE "${CMAKE_CURRENT_SOURCE_DIR}/hive_metastore.thrift")
set(FB303_FILE "${CMAKE_CURRENT_SOURCE_DIR}/share/fb303/if/fb303.thrift")
set(THRIFT_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/gen-cpp")
file(MAKE_DIRECTORY ${THRIFT_OUT_DIR})

set(THRIFT_GENERATED_FILES
    ${THRIFT_OUT_DIR}/hive_metastore_types.h
    ${THRIFT_OUT_DIR}/hive_metastore_types.cpp
    ${THRIFT_OUT_DIR}/hive_metastore_constants.h
    ${THRIFT_OUT_DIR}/hive_metastore_constants.cpp
    ${THRIFT_OUT_DIR}/ThriftHiveMetastore.h
    ${THRIFT_OUT_DIR}/ThriftHiveMetastore.cpp
    ${THRIFT_OUT_DIR}/fb303_types.h
    ${THRIFT_OUT_DIR}/fb303_types.cpp
    ${THRIFT_OUT_DIR}/FacebookService.h
    ${THRIFT_OUT_DIR}/FacebookService.cpp)

# Note: We need to include the current directory so that the "include" in
# hive_metastore.thrift finds fb303.thrift
add_custom_command(
  OUTPUT ${THRIFT_GENERATED_FILES}
  COMMAND ${THRIFT_COMPILER} --gen cpp -I ${CMAKE_CURRENT_SOURCE_DIR} -o
          ${CMAKE_CURRENT_BINARY_DIR} ${THRIFT_FILE}
  COMMAND ${THRIFT_COMPILER} --gen cpp -o ${CMAKE_CURRENT_BINARY_DIR}
          ${FB303_FILE}
  DEPENDS ${THRIFT_FILE} ${FB303_FILE}
  COMMENT "Generating Thrift files from ${THRIFT_FILE} and ${FB303_FILE}")

add_library(hms_thrift_lib OBJECT ${THRIFT_GENERATED_FILES})
target_include_directories(hms_thrift_lib PUBLIC ${THRIFT_OUT_DIR})
target_link_libraries(hms_thrift_lib PUBLIC thrift::thrift)
target_compile_options(hms_thrift_lib PRIVATE -Wno-unused-parameter
                                              -Wno-missing-field-initializers)

# Expose the generated directory to parent
set(HMS_THRIFT_INCLUDE_DIR
    ${THRIFT_OUT_DIR}
    PARENT_SCOPE)
