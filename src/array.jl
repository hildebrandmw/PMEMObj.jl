module PersistentArrays

using ..Lib

#####
##### PersistentArray
#####

mutable struct PersistentArray{T,N} <: DenseArray{T,N}
    base::Ptr{T} 
    size::Ptr{NTuple{N,Int64}} 
end

function PersistentArray{T}(size::NTuple{N,<:Integer}) where {T,N}
    # Transactionally allocate space for the base and size pointers
    baseptr, sizeptr = transaction(Pool[]) do
        baseoid = Lib.tx_zalloc(prod(size) * sizeof(T), T)
        sizeoid = Lib.tx_alloc(sizeof(size), typeof(size))

        baseptr = Lib.direct(baseoid)
        sizeptr = Lib.direct(sizeoid)

        unsafe_store!(sizeptr, size)
        return baseptr, sizeptr
    end

    array = PersistentArray{T,N}(baseptr, sizeptr)  
    finalizer(_free, array)
    return array
end

# Finalizer to free the memory occupied by the array
function _free(P::PersistentArray)
    pool = Lib.pool(P.base)
    transaction(pool) do
        Lib.tx_free(Lib.oid(P.base))
        Lib.tx_free(Lib.oid(P.size))
    end
    return nothing
end

#####
##### Array Interface Methods
#####

# Should maintain invariant that the unsafe_load is always valid.
size(P::PersistentArray) = unsafe_load(P.size)
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

end
