# - Try to find snappy headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(TBB)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  TBB_ROOT_DIR Set this variable to the root installation of
#                    snappy if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#

MESSAGE(STATUS ${TBB_ROOT_DIR})
find_library(TBB_LIBRARIES
    NAMES tbb
    HINTS ${TBB_ROOT_DIR}/lib/intel64/gcc4.8
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB DEFAULT_MSG
    TBB_LIBRARIES
)

mark_as_advanced(
    TBB_ROOT_DIR
    TBB_LIBRARIES
)
