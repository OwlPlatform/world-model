world-model
===========

  World Model abstractions and implementations for the the Owl platform/GRAIL systems.

Dependencies
------------

  This requires libowl-common, which can be git cloned as followed:
    git clone https://github.com/OwlPlatform/cpp-owl-common.git

  There are additional dependencies depending upon which database backend you want to you use.

###for SQLite World Model###
  You will require libsqlite3
###for MySQL/MariaDB World Model###
  You will require libmysqlclient, libssl, librt, and libz.

Building
--------

  There are currently two implementation of the world model, a SQLITE3 world
  model and a version based on MySQL/MariaDB.

  First initialize everything by running:
    cmake

  To build all of the versions of the world model, you can run the command
    make

  If you want to build an individual world model then run

###for SQLite World Model###
    make sqlite3_world_model_server
###for MySQL/MariaDB World Model###
    make mysql_world_model_server

