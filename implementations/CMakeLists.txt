SET(Sqlite3SourceFiles
  sqlite3_world_model.cpp
  sqlite_regexp_module.cpp
	regex_store.cpp
)

SET(MysqlSourceFiles
  mysql_world_model.cpp
)

add_library (sqlite3wm STATIC ${Sqlite3SourceFiles})
add_library (mysqlwm STATIC ${MysqlSourceFiles})

