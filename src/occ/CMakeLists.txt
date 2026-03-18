#This is the main CMAKE for build all components.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_occ_SRC)

# Include sources under occ-opt (optimized OCC helpers such as delta table).
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/../occ-opt TGT_occ_SRC)

set(TGT_occ_INC
        ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
        ${PROJECT_SRC_DIR}/include/iprange
        ${PROJECT_SRC_DIR}/include/libcomm
        ${PROJECT_SRC_DIR}/include
        ${PROJECT_SRC_DIR}/lib/gstrace
        ${LIBCGROUP_INCLUDE_PATH}
        ${PROJECT_SRC_DIR}/include/libcomm
        ${ZLIB_INCLUDE_PATH}
        ${LIBCURL_INCLUDE_PATH}
        )

set(occ_DEF_OPTIONS ${MACRO_OPTIONS})
set(occ_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(occ_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_occ TGT_occ_SRC TGT_occ_INC "${occ_DEF_OPTIONS}" "${occ_COMPILE_OPTIONS}" "${occ_LINK_OPTIONS}")
