#####
##### PersistentArray
#####

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
        baseoid = Lib.tx_zalloc(allocsize, T)
    
        persist(pool, ArrayHandle{T,N}(size, baseoid))
    end

    return PersistentArray(handle)
end

function PersistentArray(handle::Persistent{ArrayHandle{T,N}}) where {T,N}
    # We can instantiate the array directly from the handle
    size = handle.size
    base = Lib.direct(handle.base)
    return PersistentArray{T,N}(handle, size, base)
end

function PersistentArray(ptr::Ptr{PersistentArray{T,N}}) where {T,N}
    handle = unsafe_load(convert(Ptr{Persistent{ArrayHandle{T,N}}}, ptr))
    return PersistentArray(handle)
end

#####
##### Custom customizations
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

# Should maintain invariant that the unsafe_load is always valid.
size(P::PersistentArray) = P.size
sizeof(P::PersistentArray{T}) where {T} = prod(P.size) * sizeof(T)

getindex(P::PersistentArray, i::Integer) = unsafe_load(P.base, i)
setindex!(P::PersistentArray, v, i::Integer) = _safe_setindex!(P, v, i)
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
