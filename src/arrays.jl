module Arrays
#####
##### PersistentArray
#####

export PersistentArray

using ..Lib
using ..Transaction
using ..Persist

import Base: size, sizeof, getindex, setindex!, IndexStyle, pointer, unsafe_convert, similar, elsize

struct ArrayHandle{T,N}
    size::NTuple{N, Int}
    base::PersistentOID{T} 
end

mutable struct PersistentArray{T,N} <: DenseArray{T,N}
    # A handle to the underlying base for the array
    handle::Persistent{ArrayHandle{T,N}}

    # Cache the size and pointer to the base, these elements are volatile.
    size::NTuple{N, Int}
    base::Ptr{T}
end

function PersistentArray{T}(pool, size::NTuple{N, <:Integer}) where {T,N}
    handle = transaction(pool) do
        # Allocate space for the array in persistent memory
        allocsize = prod(size) * sizeof(T)
        baseoid = Lib.tx_alloc(allocsize, T)
    
        persist(pool, ArrayHandle{T,N}(size, baseoid))
    end
    return PersistentArray(handle)
end

function PersistentArray(handle::Persistent{ArrayHandle{T,N}}) where {T,N}
    _handle = retrieve(handle)

    # We can instantiate the array directly from the handle
    size = _handle.size
    base = Lib.direct(_handle.base)
    return PersistentArray{T,N}(handle, size, base)
end

#####
##### customizations
#####

function Transaction.transaction(f, P::PersistentArray{T}) where {T}
    pool = getpool(P.handle)
    return transaction(pool) do
        Lib.add_range_direct(P.base, prod(size(P)) * sizeof(T))
        f()
    end
end

#####
##### Array Interface Methods
#####

# Conversion
pointer(P::PersistentArray) = P.base
unsafe_convert(::Type{Ptr{T}}, P::PersistentArray{T}) where {T} = pointer(P)

# Should maintain invariant that the unsafe_load is always valid.
size(P::PersistentArray) = P.size
sizeof(P::PersistentArray{T}) where {T} = prod(P.size) * sizeof(T)
elsize(P::PersistentArray{T}) where {T} = sizeof(T)

getindex(P::PersistentArray, i::Integer) = unsafe_load(P.base, i)
setindex!(P::PersistentArray, v, i::Integer) = _unsafe_setindex!(P, v, i)
IndexStyle(::Type{<:PersistentArray}) = IndexLinear()

function _safe_setindex!(P::PersistentArray{T}, v, i::Integer) where {T}
    # Get the object pool backing this array
    pool = Lib.pool(P.base)      

    # Store the result in a transaction.
    transaction(pool) do
        Lib.add_range_direct(P.base + i - 1, sizeof(T))
        unsafe_store!(P.base, v, i)
    end
    return nothing
end

_unsafe_setindex!(P::PersistentArray{T}, v, i::Integer) where {T} = unsafe_store!(P.base, v, i)

# Similar
function similar(array::PersistentArray{T,N}, ::Type{U}, dims::NTuple{D,Int64}) where {T,N,U,D}
    # Store this in the same pool as the original array
    pool = Persist.getpool(array.handle)
    return PersistentArray{U}(pool, dims)
end

end
