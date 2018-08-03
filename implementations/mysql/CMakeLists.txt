SET(MysqlSourceFiles
  mysql_world_model.cpp
)

find_library(MYSQL_LIB NAMES mysqlclient mysqlclient_r)
if (MYSQL_LIB)
  # Create a static library for portable installations
  add_library (mysqlwm STATIC ${MysqlSourceFiles})
endif()
