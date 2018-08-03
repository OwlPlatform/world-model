SET(Sqlite3SourceFiles
  sqlite3_world_model.cpp
  sqlite_regexp_module.cpp
	regex_store.cpp
)

find_library(SQLITE3_LIBRARY NAMES sqlite3)
# Create a portable library for the sqlite3 world model
if (SQLITE3_LIBRARY)
  add_library (sqlite3wm ${Sqlite3SourceFiles})
  target_include_directories(sqlite3wm PUBLIC "include")
endif()

