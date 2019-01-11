module Lib

export PersistentOID, ObjectPool

# Wrapper for libpmemobj
using Libdl

# constants
const _DEPS = joinpath(@__DIR__, "..", "deps")
const _LIB = joinpath(_DEPS, "usr", "lib")
const _INCLUDE = joinpath(_DEPS, "usr", "include")

const libpmem = joinpath(_LIB, "libpmem.so")
const libpmemobj = joinpath(_LIB, "libpmemobj.so")

# dependencies
function __init__()
    global libpmem
    global libpmemobj
    Libdl.dlopen(libpmem, Libdl.RTLD_GLOBAL)
    Libdl.dlopen(libpmemobj, Libdl.RTLD_GLOBAL)
end

_typenum(::Type{T}) where {T} = hash(T)

# Interface into the low level C calls.
#
# Implemented Functions
#
# pmemobj_open
# pmemobj_close
# pmemobj_create
# pmemobj_check
#
# pmemobj_root
#
# pmemobj_tx_begin
# pmemobj_tx_end
# pmemobj_tx_stage
# pmemobj_tx_commit
# pmemobj_tx_abort
#
# pmemobj_tx_alloc
#
# List of calls that probably have to get made at some point:
#
# pmemobj_tx_zalloc
# pmemobj_type_num
# pmemobj_tx_realloc
# pmemobj_tx_zrealloc
# pmemobj_tx_free
# pmemobj_direct
# pmemobj_tx_add_range_direct
# pmemobj_first
# pmemobj_next
#
#
### Constants
#
# PMEMOBJ_MIN_POOL
# PMEMOBJ_MAX_ALLOC_SIZE

function check_version(major, minor)
    result = ccall((:pmemobj_check_version, libpmemobj), Cstring, (Cint, Cint), major, minor)
    #result = @cxx pmemobj_check_version(major, minor)
    # Returned result is null if we're good to go.
    # Otherwise, it's a Cstring and we have an error
    if result != C_NULL
        error(unsafe_string(result))
    end
    return nothing
end

#####
##### Pool creation and handling
#####

geterrno() = unsafe_load(cglobal((:errno, libpmemobj), Int32))

# Wrapper for
struct ObjectPool
    ptr::Ptr{Nothing}
end
isnull(pool::ObjectPool) = pool.ptr == C_NULL

@enum TX_Param::Int32 begin
    TX_PARAM_NONE
    #TX_PARAM_MUTEX
    #TX_PARAM_RWLOCK
    #TX_PARAM_CB
end

@enum TX_Stage::Int32 begin
    TX_STAGE_NONE
    TX_STAGE_WORK
    TX_STAGE_ONCOMMIT
    TX_STAGE_ONABORT
    TX_STAGE_FINALLY
end

"""
    Persistent Object ID


Implementation Notes
--------------------
Matches the layout of

```c
typedef struct pmemoid {
    uint64_t pool_uuid_lo;
    uint64_t off;
} PMEMoid;
```
in `pmdk/src/include/libpmemobj/base.h`, line 108-111. Since this type is composed of
bits-type objects, it can be passed back and forth by value to the C code.
"""
struct PersistentOID{T}
    pool_uuid_lo::UInt64
    off::UInt64
end
isnull(oid::PersistentOID) = iszero(oid.pool_uuid_lo) && iszero(oid.off)

#####
##### pmemobj_open
#####

# These are are funcions defined in:
# http://pmem.io/pmdk/manpages/linux/v1.5/libpmemobj/pmemobj_open.3

"""
    close(pool::ObjectPool)

Close the memory pool indicated by `pool` and delete the memory pool handle. The object
store itself lives on in the file that contains it and may be reopened at a later time using
[`open`](@ref).
"""
function close(pool::ObjectPool)
    ccall(
        (:pmemobj_close, libpmemobj),
        Cvoid,
        (Ptr{Nothing},),
        pool.ptr
    )
end

"""
    create(path, poolsize; layout = "", mode = UInt32(0o666) -> ObjectPool

Create a transactional object store with the given total `poolsize` in bytes. Argument
`path` specifies the name of the memory pool file to be created. Argument `layout` specifies
the application's layout type in the form of a string. The layout name is not interpreted by
`libpmemobj`, but may be used as a check when [`pmemobj_open`](@ref`) is called.
"""
function create(path, poolsize; layout = "", mode = UInt32(0o0666))
    ptr = ccall(
        (:pmemobj_create, libpmemobj),
        Ptr{Nothing},
        (Cstring, Cstring, Csize_t, Base.Cmode_t),
        path, layout, poolsize, mode
    )
    return ObjectPool(ptr)
end

"""
    open(path; layout = "") -> ObjectPool

Open an existing object store memory pool. Similar to [`create`](@ref), `path` must identify
either an existing object memory pool file.

Return an handle `ObjectPool`. If allocation fails, returns a null `ObjectPool` which can
be tested with `isnull`.
"""
function open(path; layout = "")
    ptr = ccall(
        (:pmemobj_open, libpmemobj),
        Ptr{Nothing},
        (Cstring, Cstring),
        path, layout,
    )
    return ObjectPool(ptr)
end

"""
    check(path; layout = "") -> Bool

Perform a consistency check of the file indicated by `path`. Return `true` if the file
passes the check.
"""
function check(path; layout = "")
    val = ccall(
        (:pmemobj_check, libpmemobj),
        Cint,
        (Cstring, Cstring),
        path, layout
    )
    # According to the man page, a return value of zero indicates that something
    # went wrong
    return val != 0
end

#####
##### oid_is_null
#####

# http://pmem.io/pmdk/manpages/linux/v1.5/libpmemobj/oid_is_null.3

"""
    direct(oid::PersistentOID{T}) -> Ptr{T}

Return a pointer to the persistent memory object with the handle `oid`.
"""
function direct(oid::PersistentOID{T}) where {T}
    ccall((:pmemobj_direct, libpmemobj), Ptr{T}, (PersistentOID{T},), oid)
end

"""
    oid(ptr::Ptr{T}) -> PersistentOID{T}

Return a [`PersistentOID{T}`](@ref) handle to the object pointed to by `ptr`.
"""
oid(ptr::Ptr{T}) where {T} = ccall(
    (:pmemobj_oid, libpmemobj),
    PersistentOID{T},
    (Ptr{Cvoid},),
    ptr
)

"""
    pool(ptr::Ptr) -> ObjectPool

Return a [`ObjectPool`](@ref) handle to the pool containing the object containt the addresses
pointed to by `ptr`.
"""
function pool(ptr::Ptr)
    pool = ccall(
        (:pmemobj_pool_by_ptr, libpmemobj),
        Ptr{Cvoid},
        (Ptr{Cvoid},),
        ptr
    )
    return ObjectPool(pool)
end

"""
    pool(oid::PersistentOid} -> ObjectPool

Return an [`ObjectPool`](@ref) handle to the pool containing the object with handle `oid`.
"""
function pool(oid::PersistentOID{T}) where {T}
    pool = ccall(
        (:pmemobj_pool_by_oid, libpmemobj),
        Ptr{Cvoid},
        (PersistentOID{T},),
        oid
    )
    return ObjectPool(pool)
end

#####
##### pmemobj_root
#####

# http://pmem.io/pmdk/manpages/linux/v1.5/libpmemobj/pmemobj_root.3

"""
    root(pool, size, [::Type{T}]) -> PersistentOID{T}

Create or resize the root object for the persistent memory pool `pool`.  Return a
[`PersistentOID{T}`](@ref) handle to the allocated root object. If `T` is not provided,
default to `Nothing`.

*   If this is the first call to `root`, the requested size is greater than zero and the
    root object does not exist, it is implicitly allocated in a thread-safe manner.

*   If the requested size is larger than the current size, the root object is automatically
    resized. In such case, the old data is preserved and the extra space is zeroed.

*   If the requested size is equal to zero, the root object is not allocated.
"""
function root(pool::ObjectPool, size, ::Type{T} = Nothing) where {T}
    oid = ccall(
        (:pmemobj_root, libpmemobj),
        PersistentOID{T},
        (Ptr{Cvoid}, Csize_t),
        pool.ptr, size
    )
    return oid
end

"""
    root_size(pool::ObjectPool) -> Int

Return the size in bytes of the root object of `pool`.
"""
function root_size(pool::ObjectPool)
    size = ccall(
        (:pmemobj_root_size, libpmemobj),
        Csize_t,
        (Ptr{Cvoid},),
        pool.ptr
    )
    return Int(size)
end

#####
##### pmemobj_tx_begin
#####


# TODO: Last argument is likely wrong.
# Need to deal with the "enums" directly
function tx_begin(pop::ObjectPool)
    ret = ccall(
        (:pmemobj_tx_begin, libpmemobj),
        Cint,
        (Ptr{Cvoid}, Ptr{Cvoid}, Cint),
        pop.ptr, C_NULL, TX_PARAM_NONE
    )

    ret == 0 || throw(error("Failed to begin transaction"))
    return ret
end

tx_abort(errnum::Integer) = ccall((:pmemobj_tx_abort, libpmemobj), Cvoid, (Cint,), errnum)
tx_commit() = ccall((:pmemobj_tx_commit, libpmemobj), Cvoid, ())
tx_end() = ccall((:pmemobj_tx_end, libpmemobj), Cint, ())
tx_process() = ccall((:pmemobj_tx_process, libpmemobj), Cvoid, ())

function tx_stage()
    stage = ccall((:pmemobj_tx_stage, libpmemobj), Cint, ())
    return TX_Stage(stage)
end

#####
##### pmemobj_tx_alloc
#####

# http://pmem.io/pmdk/manpages/linux/v1.5/libpmemobj/pmemobj_tx_alloc.3
#
function tx_alloc(size, ::Type{T} = Nothing, typenum = _typenum(T)) where {T}
    pmemoid = ccall(
        (:pmemobj_tx_alloc, libpmemobj),
        PersistentOID{T},
        (Csize_t, Culonglong),
        size, typenum
    )
    return pmemoid
end

function tx_zalloc(size, ::Type{T} = Nothing, typenum = _typenum(T)) where {T}
    pmemoid = ccall(
        (:pmemobj_tx_zalloc, libpmemobj),
        PersistentOID{T},
        (Csize_t, Culonglong),
        size, typenum
    )
    return pmemoid
end

function tx_free(oid::PersistentOID{T}) where {T}
    ret = ccall(
        (:pmemobj_tx_free, libpmemobj),
        Cint,
        (PersistentOID{T},),
        oid
    )
    ret != 0 && throw(error("Unable to free object: $oid"))
    return nothing
end

function tx_strdup(str, ::Type{T} = UInt8) where {T}
    typenum = _typenum(T)
    oid = ccall(
        (:pmemobj_tx_strdup, libpmemobj),
        PersistentOID{T},
        (Cstring, Culonglong),
        str, typenum
    )
    return oid
end

#####
##### pmemobj_tx_add_range
#####

# http://pmem.io/pmdk/manpages/linux/v1.5/libpmemobj/pmemobj_tx_add_range.3
function add_range(oid::PersistentOID{T}, off, size) where {T}
    ret = ccall(
        (:pmemobj_tx_add_range, libpmemobj),
        Cint,
        (PersistentOID{T}, Culonglong, Csize_t),
        oid, off, size
    )
    ret != 0 && throw(error("Adding failed: error: $ret"))
    return nothing
end

function add_range_direct(ptr::Ptr, size)
    ret = ccall(
        (:pmemobj_tx_add_range_direct, libpmemobj),
        Cint,
        (Ptr{Cvoid}, Csize_t),
        ptr, size
    )
    ret != 0 && throw(error("Add range direct failed: error: $ret"))
    return nothing
end


#####
##### pmemobj_memcpy_persist
#####

# http://pmem.io/pmdk/manpages/linux/v1.5/libpmemobj/pmemobj_memcpy_persist.3
function persist(pool::ObjectPool, addr, size)
    ccall(
        (:pmemobj_persist, libpmemobj),
        Cvoid,
        (Ptr{Cvoid}, Ptr{Cvoid}, Csize_t),
        pool.ptr, addr, size
    )
end

#####
##### pmemobj_alloc
#####

# http://pmem.io/pmdk/manpages/linux/v1.5/libpmemobj/pmemobj_alloc.3

function _constructor(pool_pointer::Ptr{Nothing}, obj_pointer::Ptr{T}, arg_pointer::Ptr{T}) where {T}
    unsafe_store!(obj_pointer, unsafe_load(arg_pointer))
    ccall(
        (:pmemobj_persist, libpmemobj),
        Cvoid,
        (Ptr{Cvoid}, Ptr{Cvoid}, Csize_t),
        pool_pointer, obj_pointer, sizeof(T)
    )
    return Int32(0)
end

function alloc(pop::ObjectPool, x::T) where {T}
    size = sizeof(T)
    typenum = _typenum(T)

    oidp = Ref{PersistentOID{T}}()
    f = @cfunction(_constructor, Cint, (Ptr{Cvoid}, Ptr{T}, Ptr{T}))

    ret = ccall(
        (:pmemobj_alloc, libpmemobj),
        Cint,
        (Ptr{Cvoid}, Ptr{Cvoid}, Csize_t, Culonglong, Ptr{Cvoid}, Ptr{Cvoid}),
        pop.ptr, oidp, size, typenum, f, Ref(x)
    )
    ret != 0 && throw(error("Allocation failed"))

    return (ret, oidp[])
end

function zalloc(pop::ObjectPool, size, ::Type{T} = Nothing) where {T}
    typenum = _typenum(T)
    oidp = Ref{PersistentOID{T}}()

    ret = ccall(
        (:pmemobj_zalloc, libpmemobj),
        Cint,
        (Ptr{Cvoid}, Ptr{PersistentOID{T}}, Csize_t, Culonglong),
        pop.ptr, oidp, size, typenum
    )
    ret != 0 && throw(error("Allocation failed"))

    return oidp[]
end

function free(oid::PersistentOID{T}) where {T}
    ccall(
        (:pmemobj_free, libpmemobj),
        Cvoid,
        (Ptr{PersistentOID{T}},),
        Ref(oid)
    )

end

function alloc_usable_size(oid::PersistentOID{T}) where {T}
    return ccall(
        (:pmemobj_alloc_usable_size, libpmemobj),
        Csize_t,
        (PersistentOID{T},),
        oid,
    )
end

function strdup(pool::ObjectPool, str, ::Type{T} = UInt8) where {T}
    typenum = _typenum(T)
    oidp = Ref{PersistentOID{T}}()

    ret = ccall(
        (:pmemobj_strdup, libpmemobj),
        Cint,
        (Ptr{Cvoid}, Ptr{PersistentOID{T}}, Cstring, Culonglong),
        pool.ptr, oidp, str, typenum
    )
    ret != 0 && throw(error("Allocation failed"))

    return oidp[]
end

end # module Lib
