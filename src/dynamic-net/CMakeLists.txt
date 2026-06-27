# CMakeLists.txt for dynamic-net (link from epoch/ when integrated)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_dynamic_net_SRC)

set(dynamic_net_INC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SRC_DIR}/include
        )

set(dynamic_net_DEF_OPTIONS ${MACRO_OPTIONS})
set(dynamic_net_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(dynamic_net_LINK_OPTIONS ${BIN_LINK_OPTIONS})

# Placeholder target name — wire into gausskernel/epoch when integrating
add_static_objtarget(gausskernel_storage_access_dynamic_net TGT_dynamic_net_SRC dynamic_net_INC
    "${dynamic_net_DEF_OPTIONS}" "${dynamic_net_COMPILE_OPTIONS}" "${dynamic_net_LINK_OPTIONS}")
