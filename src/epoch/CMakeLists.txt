#This is the main CMAKE for build epoch multi-master module.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_epoch_SRC)

set(TGT_epoch_INC
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

set(epoch_DEF_OPTIONS ${MACRO_OPTIONS})
set(epoch_LINK_OPTIONS ${BIN_LINK_OPTIONS})

# HCJ ADDBY: optional ZMQ from GeoGauss build output
set(EPOCH_ZMQ_ROOT "/data/geogauss/zmqroot" CACHE PATH "Root path of libzmq install")
if(EXISTS "${EPOCH_ZMQ_ROOT}/lib/libzmq.so")
    list(APPEND TGT_epoch_INC ${EPOCH_ZMQ_ROOT}/include)
    list(APPEND epoch_DEF_OPTIONS -DEPOCH_USE_ZMQ=1)
    list(APPEND epoch_LINK_OPTIONS -L${EPOCH_ZMQ_ROOT}/lib -lzmq)
    message(STATUS "epoch: enable ZMQ from ${EPOCH_ZMQ_ROOT}")
else()
    message(STATUS "epoch: ZMQ not found, multi-node network disabled at compile time")
endif()

set(epoch_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_epoch TGT_epoch_SRC TGT_epoch_INC "${epoch_DEF_OPTIONS}" "${epoch_COMPILE_OPTIONS}" "${epoch_LINK_OPTIONS}")
