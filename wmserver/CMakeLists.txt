SET(SourceFiles
  thread_connection.cpp
  world_model_server.cpp
	request_state.cpp
)

add_executable (sqlite3_world_model_server ${SourceFiles})
target_link_libraries (sqlite3_world_model_server owl-common owlwm sqlite3wm sqlite3)

add_executable (mysql_world_model_server ${SourceFiles})
target_link_libraries (mysql_world_model_server owl-common owlwm mysqlwm mysqlclient_r ssl rt z crypto pthread dl)

#set(SQLITE_FLAGS " -lpthread -lsqlite3 -ldl")
#set(MYSQL_FLAGS " -lpthread -lmysqlclient_r -ldl -lz -lssl -lrt")

#set_target_properties(sqlite3_world_model_server PROPERTIES COMPILE_FLAGS "${SQLITE_FLAGS}")
#set_target_properties(mysql_world_model_server PROPERTIES COMPILE_FLAGS "${MSQYL_FLAGS}")
set_target_properties(mysql_world_model_server PROPERTIES COMPILE_DEFINITIONS "USE_MYSQL")

INSTALL(TARGETS sqlite3_world_model_server RUNTIME DESTINATION bin/owl OPTIONAL)
INSTALL(TARGETS mysql_world_model_server RUNTIME DESTINATION bin/owl OPTIONAL)

