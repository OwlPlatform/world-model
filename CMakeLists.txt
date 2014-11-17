cmake_minimum_required (VERSION 2.8)
project (OwlWM)

# The version number.
set (LibOwlWM_VERSION_MAJOR 1)
set (LibOwlWM_VERSION_MINOR 0)
set (LibOwlWM_VERSION_REVISION 0)

#Use these to change to clang for the compiler (or set the CC and CXX variables)
#SET (CMAKE_C_COMPILER "/usr/bin/clang")
#SET (CMAKE_CXX_COMPILER "/usr/bin/clang++")

execute_process(COMMAND git diff-tree -s --pretty="%aD, hash %h" HEAD
	OUTPUT_VARIABLE REPO_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)

add_definitions(-DGIT_REPO_VERSION=${REPO_VERSION})

#Use optimization level three for the world model libraries
set(CMAKE_CXX_FLAGS "-O3 -std=c++0x -Wall -Wextra -Wno-sign-compare")
#Use this to switch to building debugging code
#set(CMAKE_CXX_FLAGS "--debug -g -std=c++0x -Wall -Wextra -Wno-sign-compare")

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

#Let the user choose which implementations to build
SET(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY TRUE)

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

#Note that not all distributions put init script in /etc/init.d (Slackware is in /etc/rc.d)
INSTALL( FILES ${OwlWM_SOURCE_DIR}/scripts/rc.owl-world-model PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE DESTINATION /etc/init.d )

#Install the configuration script
INSTALL( FILES ${OwlWM_SOURCE_DIR}/scripts/mysqlwm.conf PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ DESTINATION /etc/owl )

# uninstall target
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)

