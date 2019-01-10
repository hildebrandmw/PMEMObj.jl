# These objects live on the heap, so the must be mutable.
#
# We can use finalizers to hook these objects into Julia's garbage collector.
mutable struct Persistent{T}
    ptr::Ptr{T}
end

Base.get(p::Persistent{T}) where {T} = unsafe_load(p.ptr)
Base.show(io::IO, p::Persistent{T}) where {T}  = println(io, "Persistent $T: $(get(p))")

function persistent(pool::ObjectPool, x::T) where {T}
    isbitstype(T) || throw(error("Can only persist \"isbits\" types."))

    ptr = _alloc(pool, x)
    return Persistent{T}(ptr)
end

#####
##### Persistent Array
#####

const OVERALLOCATION_FACTOR = 3
const MIN_ALLOCATION_SIZE = 2^24

function allocation_bytes(size, ::Type{T}) where {T}
    max(MIN_ALLOCATION_SIZE, OVERALLOCATION_FACTOR * prod(size) * sizeof(T))
end

struct ArrayRoot{T,N}
    size::NTuple{N,Int64}
    base_oid::OID{T}
end

mutable struct PersistentArray{T,N} <: AbstractArray{T,N}
    pool::ObjectPool
    size::NTuple{N,Int64}
    base::Ptr{T}
    isclosed::Bool
end

function _close(P::PersistentArray) 
    if !P.isclosed
        _close(P.pool)
        P.isclosed = true
    end
end

function PersistentArray{T}(file::AbstractString, size::NTuple{N,Int64}) where {T,N}
    # Create an object pool
    #
    # Must allocate more than will actually be needed by the array to allow room for the
    # snapshots.

    pool = _create(file, "test", allocation_bytes(size, T))

    # Construct the root from the size and base oid
    roottype = ArrayRoot{T,N}
    root_oid = _root(pool, sizeof(roottype), roottype)
    root_pointer = _direct(root_oid)

    # Allocate room for the array
    base_oid = _zalloc(pool, prod(size) * sizeof(T), T)

    root = ArrayRoot(size, base_oid)
    transaction(pool) do
        _add_range_direct(root_pointer, sizeof(root))
        unsafe_store!(root_pointer, root)
    end

    base_pointer = _direct(base_oid)

    # Construct the object and mark it to close when gc'd
    array = PersistentArray{T,N}(pool, size, base_pointer, false)
    finalizer(_close, array)
    return array
end

function PersistentArray{T,N}(file::AbstractString) where {T,N}
    pool = _open(file, "test")

    # Open up the root pointer
    roottype = ArrayRoot{T,N}
    sizetype = NTuple{N, Int64}
    root_oid = _root(pool, sizeof(roottype), roottype)
    root_pointer = _direct(root_oid)

    # Construct a pointer to the base oid
    root = unsafe_load(root_pointer)
    size = root.size
    base_pointer = _direct(root.base_oid)
    array = PersistentArray{T,N}(pool, size, base_pointer, false)
    finalizer(_close, array)
    return array
end

Base.pointer(P::PersistentArray{T}) where {T} = P.base
pool(P) = P.pool

Base.size(P::PersistentArray) = P.size
Base.sizeof(P::PersistentArray{T}) where {T} = length(P) * sizeof(T)
Base.getindex(P::PersistentArray, i) = unsafe_load(P.base, i)


# For now, this must be conservative for correctness.
#
# Something to do is to use Cassette to supply contexts for removing the inner transaction
# if it detects that it's in an outer transaction.
Base.setindex!(P::PersistentArray, x, i::Integer) = _safe_setindex!(P, x, i)

# Call this if already inside a "transaction" block to avoid starting a new transaction.
function _safe_setindex!(P::PersistentArray{T}, x, i::Integer) where {T}
    # Can't do _persist because T may not be 8 bytes (maximum for atomic transfers)
    transaction(pool(P)) do
        _add_range_direct(pointer(P) + i - 1, sizeof(T))
        unsafe_store!(pointer(P), x, i)
    end
    return nothing
end

function _unsafe_setindex!(P::PersistentArray, x, i::Integer) 
    unsafe_store!(P.base, x, i)
    return nothing
end

macro persistent(array, body)
    array = esc(array)
    # Make a closure around the body
    closure = :(@closure () -> $(esc(body))) 
    return quote
        transaction(pool($array)) do
            _add_range_direct(pointer($array), sizeof($array))
            ctx = Cassette.disablehooks(TransactionCtx(metadata = $array))
            return Cassette.overdub(ctx, $closure)
        end
    end
end


# Cassette context for eliding scheduling
Cassette.@context TransactionCtx
function Cassette.overdub(ctx::TransactionCtx, ::typeof(_safe_setindex!), P::PersistentArray, x, i) 
    # Only intercept if we're referring to our specific array
    #P === ctx.metadata ? _unsafe_setindex!(P, args...) : _safe_setindex!(P, args...)
    _unsafe_setindex!(P, x, i)
end

# Just mark the whole array for the transaction.
#
# Then, intercept all setindex! function calls targeting THIS persistent array and swap the
# implementation to `_unsafe_setindex!`
function transaction(f, P::PersistentArray)
    transaction(pool(P)) do
        ctx = TransactionCtx(metadata = P)
        # Don't need prehook or posthook for this contextual execution pass.
        Cassette.disablehooks(ctx)

        _add_range_direct(pointer(P), sizeof(P))
        Cassette.overdub(ctx, f)
    end
end


############################################################################################
function Base.copyto!(dest::Array{T,N}, src::PersistentArray{T,N}) where {T,N}
    # Just to straight-up copy-to right now
    unsafe_copyto!(pointer(dest), src.base, length(src))
    return nothing
end

function Base.copyto!(dest::PersistentArray{T,N}, src::Array{T,N}) where {T,N}
    # TODO: Boundscheck Macro?
    #  Maybe make that the "unsafe_copyto" variant
    if length(dest) < length(src)
        throw(error("Destination must be longer than source"))
    end

    transaction(dest) do
        unsafe_copyto!(pointer(dest), pointer(src), length(src))
    end

    return nothing
end
