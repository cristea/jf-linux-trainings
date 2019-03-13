# -----------------------------------------------------------------
# JF_LIBRARY(
#   NAME <library-name>
#   DEPENDENCIES <libraries that we depend on (optional)>
#   HEADERS <header files defining interfaces into the library (optional)>
#   SOURCES <C and C++ implementation files>
# )
# -----------------------------------------------------------------
FUNCTION(JF_LIBRARY)
  SET(options)
  SET(oneValueArgs NAME)
  SET(multiValueArgs DEPENDENCIES HEADERS SOURCES)
  CMAKE_PARSE_ARGUMENTS(JF_LIBRARY "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  if (NOT  "${JF_LIBRARY_UNPARSED_ARGUMENTS}" STREQUAL "")
    MESSAGE(FATAL_ERROR "JF_LIBRARY: unparsed arguments (${JF_LIBRARY_UNPARSED_ARGUMENTS}) remain: ${ARGN}")
  endif()

  FIND_PACKAGE(Threads)

  ADD_LIBRARY(${JF_LIBRARY_NAME} ${JF_LIBRARY_HEADERS} ${JF_LIBRARY_SOURCES})
  TARGET_LINK_LIBRARIES(${JF_LIBRARY_NAME} ${JF_LIBRARY_DEPENDENCIES} ${CMAKE_THREAD_LIBS_INIT})

  # PIC - Position Independent Code. Needed to aggregate objects (from
  # an archive, likely) into a shared object.
  TARGET_COMPILE_OPTIONS(${JF_LIBRARY_NAME} PRIVATE -fPIC)

  INSTALL(TARGETS ${JF_LIBRARY_NAME} DESTINATION lib)
  INSTALL(FILES ${JF_LIBRARY_HEADERS} DESTINATION include)
ENDFUNCTION(JF_LIBRARY)

# -----------------------------------------------------------------
# JF_EXECUTABLE(
#   NAME <exe-name>
#   DEPENDENCIES <libraries that we depend on (optional)>
#   HEADERS <header files (optional; if given *not* installed)>
#   SOURCES <C and C++ implementation files>
# )
 # -----------------------------------------------------------------
FUNCTION(JF_EXECUTABLE)
  CMAKE_PARSE_ARGUMENTS(JF_EXECUTABLE "" "NAME" "HEADERS;SOURCES;DEPENDENCIES" ${ARGN})
  IF (NOT  "${JF_EXECUTABLE_UNPARSED_ARGUMENTS}" STREQUAL "")
    MESSAGE(FATAL_ERROR "JF_EXECUTABLE: unparsed arguments (${JF_EXECUTABLE_UNPARSED_ARGUMENTS}) remain: ${ARGN}")
  ENDIF()

  FIND_PACKAGE(Threads)

  ADD_EXECUTABLE(
    ${JF_EXECUTABLE_NAME}

    ${JF_EXECUTABLE_HEADERS}
    ${JF_EXECUTABLE_SOURCES}
    )

  TARGET_LINK_LIBRARIES(
    ${JF_EXECUTABLE_NAME}

    ${JF_EXECUTABLE_DEPENDENCIES}
    ${CMAKE_THREAD_LIBS_INIT}
    )

  INSTALL(TARGETS ${JF_EXECUTABLE_NAME} DESTINATION bin)

ENDFUNCTION(JF_EXECUTABLE)

# -----------------------------------------------------------------
# JF_TEST(
#   NAME <exe-name>
#   DEPENDENCIES <libraries that we depend on (optional)>
#   HEADERS <header files (optional; if given *not* installed)>
#   SOURCES <C and C++ implementation files>
# )
 # -----------------------------------------------------------------
FUNCTION(JF_TEST)
  CMAKE_PARSE_ARGUMENTS(JF_TEST "" "NAME" "HEADERS;SOURCES;DEPENDENCIES" ${ARGN})
  IF (NOT  "${JF_TEST_UNPARSED_ARGUMENTS}" STREQUAL "")
    MESSAGE(FATAL_ERROR "JF_TEST: unparsed arguments (${JF_TEST_UNPARSED_ARGUMENTS}) remain: ${ARGN}")
  ENDIF()

  FIND_PACKAGE(Threads)
  FIND_PACKAGE(Boost REQUIRED COMPONENTS unit_test_framework)

  ADD_EXECUTABLE(
    ${JF_TEST_NAME}

    ${JF_TEST_HEADERS}
    ${JF_TEST_SOURCES}
    )

  TARGET_LINK_LIBRARIES(
    ${JF_TEST_NAME}

    ${JF_TEST_DEPENDENCIES}
    ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    )

  ADD_TEST(
    ${JF_TEST_NAME}
    ${CMAKE_CURRENT_BINARY_DIR}/${JF_TEST_NAME}
    )

ENDFUNCTION(JF_TEST)
