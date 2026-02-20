cmake_minimum_required(VERSION 3.11)

# Set extension name here
set(TARGET_NAME hive_metastore)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

add_subdirectory(src)

build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})
build_loadable_extension(${TARGET_NAME} " " ${ALL_OBJECT_FILES})

# Link Thrift library - uses thrift::thrift target from src/thrift/CMakeLists.txt
target_link_libraries(${EXTENSION_NAME} thrift::thrift)
target_link_libraries(${LOADABLE_EXTENSION_NAME} thrift::thrift)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
