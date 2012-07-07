SET(SourceFiles
  test_world_model.cpp
)

add_executable (test_world_model ${SourceFiles})
target_link_libraries (test_world_model owl owlwm sqlite3wm sqlite3 mysqlwm mysqlclient_r ssl rt z)

