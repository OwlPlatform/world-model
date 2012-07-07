cmake_minimum_required (VERSION 2.8)
project (OwlWM)

# The version number.
set (LibOwlWM_VERSION_MAJOR 1)
set (LibOwlWM_VERSION_MINOR 0)
set (LibOwlWM_VERSION_REVISION 0)

#Use optimization level three for the world model libraries
set(CMAKE_CXX_FLAGS "-O3 -std=c++0x -Wall -Wextra -Wno-sign-compare")
#set(SQLITE_FLAGS " -lpthread -lsqlite3 -ldl")
#set(MYSQL_FLAGS " -DUSE_MYSQL -lpthread -lmysqlclient_r -ldl -lz -lssl -lrt")
#set(CMAKE_CXX_FLAGS "${BASE_CXX_FLAGS}")

FIND_PATH(OWL_INC_DIR NAMES owl PATHS /usr/include /usr/local/include)
IF(NOT OWL_INC_DIR)
  MESSAGE(FATAL_ERROR "Unable to find libowl installation!")
ELSE(NOT OWL_INC_DIR)
  #Truncate the search path
  STRING(REGEX REPLACE "/owl" "" OWL_INC_DIR ${OWL_INC_DIR})
ENDIF(NOT OWL_INC_DIR)

include_directories ("${OWL_INC_DIR}")

include_directories ("${OwlWM_SOURCE_DIR}/libwm/include")
include_directories ("${OwlWM_SOURCE_DIR}/implementations")

add_subdirectory (libwm)
add_subdirectory (implementations)
add_subdirectory (wmserver bin)
add_subdirectory (tests/src tests/bin)

#Set the correct library directory suffix
if(NOT DEFINED LIB_SUFFIX)
  get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
  if (${LIB64} STREQUAL "TRUE")
    set(LIBSUFFIX 64)
  else()
    set(LIBSUFFIX "")
  endif()
  set(LIB_SUFFIX ${LIBSUFFIX} CACHE STRING "lib directory suffix")
endif()

SET(PKGCONFIG_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}/lib${LIB_SUFFIX}/pkgconfig/" CACHE STRING "Base directory for pkgconfig files")
SET(INCLUDE_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/include/owl/" CACHE PATH "The subdirectory to the header prefix")

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/owlwm.pc.cmake ${CMAKE_CURRENT_BINARY_DIR}/owlwm.pc @ONLY)
INSTALL( FILES ${CMAKE_CURRENT_BINARY_DIR}/owlwm.pc DESTINATION ${PKGCONFIG_INSTALL_PREFIX})
#add_library (LibOwl SHARED ${IncludeFiles} ${SourceFiles})

#$(LIBDIR)/libowl.a: $(LIBDIR)/sensor_aggregator_protocol.o $(LIBDIR)/aggregator_solver_protocol.o $(LIBDIR)/world_model_protocol.o $(LIBDIR)/netbuffer.o $(LIBDIR)/sample_data.o $(LIBDIR)/simple_sockets.o

# uninstall target
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)

