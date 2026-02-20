add_library(
  hms_ext_storage OBJECT
  hms_catalog.cpp
  hms_catalog_set.cpp
  hms_clear_cache.cpp
  hms_schema_entry.cpp
  hms_schema_set.cpp
  hms_table_entry.cpp
  hms_table_set.cpp
  hms_transaction.cpp
  hms_transaction_manager.cpp)

# Link to hms_thrift_lib to get Thrift includes
target_link_libraries(hms_ext_storage PRIVATE hms_thrift_lib)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:hms_ext_storage>
    PARENT_SCOPE)
