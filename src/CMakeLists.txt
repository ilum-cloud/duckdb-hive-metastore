include_directories(include)

add_subdirectory(thrift)

add_subdirectory(storage)

add_library(
  hms_ext_library OBJECT
  hive_metastore_extension.cpp hms_utils.cpp hms_format_detector.cpp
  hms_path_utils.cpp hms_client.cpp hms_api.cpp)
# Link to hms_thrift_lib to get Thrift includes and library
target_link_libraries(hms_ext_library PRIVATE hms_thrift_lib)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:hms_ext_library>
    $<TARGET_OBJECTS:hms_thrift_lib>
    PARENT_SCOPE)
