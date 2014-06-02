SET(SourceFiles
  standing_query.cpp
  semaphore.cpp
	world_model.cpp
)

add_library (owlwm SHARED ${SourceFiles})
set (FULLVERSION ${LibOwlWM_VERSION_MAJOR}.${LibOwlWM_VERSION_MINOR}.${LibOwlWM_VERSION_REVISION})
SET_TARGET_PROPERTIES(owlwm PROPERTIES VERSION ${FULLVERSION} SOVERSION ${FULLVERSION})

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
SET(CMAKE_INSTALL_LIBDIR lib${LIB_SUFFIX} CACHE PATH "Output directory for libraries")
INSTALL(TARGETS owlwm DESTINATION ${CMAKE_INSTALL_LIBDIR})
