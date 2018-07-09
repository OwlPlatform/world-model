SET(SourceFiles
  test_world_model.cpp
)

add_executable (test_world_model ${SourceFiles})

set(TEST_LIBS owl-common owlwm sqlite3wm sqlite3 rt z pthread dl crypto)
find_library(MYSQL NAMES mysqlclient_r)
if (${MYSQL_FOUND})
  # Add mysql libraries and define if mysql world model was built
  set(TEST_LIBS "${TEST_LIBS} mysqlwm mysqlclient_r ssl")
  target_compile_definitions(test_world_model PRIVATE -DUSE_MYSQL)
endif()

target_link_libraries (test_world_model ${TEST_LIBS})

