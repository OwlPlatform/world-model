SET(Sqlite3SourceFiles
  sqlite3_world_model.cpp
  sqlite_regexp_module.cpp
)

SET(MysqlSourceFiles
  mysql_world_model.cpp
)

add_library (sqlite3wm SHARED ${Sqlite3SourceFiles})
add_library (mysqlwm SHARED ${MysqlSourceFiles})

