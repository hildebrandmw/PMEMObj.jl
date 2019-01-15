module Persist

# TODO: Implement Arrays
# TODO: Handle Circular References
# TODO: Cleanup loading and storing code
#   - Review existing constructs - there may be a simpler implementation
#   - Make generic persisting and storing type stable, either through code tricks or by
#       using @generated.
#
# TODO: write tests!

export Persistent, persist, retrieve

import Base: getproperty, setproperty!, size, getindex, setindex!, IndexStyle, sizeof

using ..Lib
using ..Transaction

abstract type AbstractClass end

struct NoClass <: AbstractClass end
struct IsBits <: AbstractClass end

class(::Type{T}) where {T} = isbitstype(T) ? IsBits() : NoClass()
class(x::T) where {T} = class(T)

"""
Persistent wrapper for (mostly) arbitrary types. The layout of a `Persistent{T}` is similar
similar to the layout of standard Julia object with the following modifications

* If a field element is `isbits`, the element is stored inline
* If a field element is not `isbits`, then we store a `PersistentOID` to that object. This
    means that fields offsets below the non `isbits` object may have to be shifted down to
    accomodate the 16 bytes required for a `PersistentOID`.
"""
struct Persistent{T}
    __oid::PersistentOID{T}
end
getoid(P::Persistent) = getfield(P, :__oid)
getpool(P::Persistent) = Lib.pool(getoid(P))

include("array.jl")

# TODO: Automagic this with a macro?
aspersistent(::Type{T}) where {T} = Persistent{T}
asvolatile(::Type{T}) where {T} = T

aspersistent(::Type{Array{T,N}}) where {T,N} = Persistent{ArrayHandle{T,N}}
asvolatile(::Type{ArrayHandle{T,N}}) where {T,N} = Array{T,N}

#####
##### persist
#####

persist(pool, x::T) where {T} = persist(pool, x, class(T))

persist(pool, x::T, trait::AbstractClass) where {T} = transaction(() -> store(pool, x, trait), pool)

#####
##### Store
#####

store(pool, x::T, ::AbstractClass) where {T <: Union{String,Symbol}} = Persistent(Lib.tx_strdup(String(x), T))

function store(pool, x::T, ::IsBits) where {T}
    oid = Lib.tx_alloc(sizeof(x), T)
    ptr = Lib.direct(oid)
    unsafe_store!(ptr, x)

    return Persistent(oid)
end

function store(pool, x::Array{T,N}, ::AbstractClass) where {T,N}
    oid = Lib.tx_alloc(sizeof(x), T)
    ptr = Lib.direct(oid)
    unsafe_copyto!(ptr, pointer(x), length(x))

    handle = persist(pool, ArrayHandle{T,N}(size(x), oid))
    return Persistent(handle)
end

#####
##### retrieve
#####

retrieve(x::Persistent{T}) where {T} = retrieve(x, class(T))
retrieve(x::Persistent, trait::AbstractClass) = retrieve(Lib.direct(getoid(x)), trait)

# Specializtions
retrieve(ptr::Ptr{T}, ::AbstractClass) where {T} = asvolatile(T)(unsafe_string(convert(Ptr{UInt8}, ptr)))
retrieve(ptr::Ptr{T}, ::IsBits) where {T} = unsafe_load(convert(Ptr{asvolatile(T)}, ptr))

# Need to mark as "isbits" since class(ArrayHandle) returns IsBits
function retrieve(ptr::Ptr{ArrayHandle{T,N}}, ::IsBits) where {T,N}
    handle = unsafe_load(ptr)
    A = asvolatile(ArrayHandle{T,N})(undef, handle.size)
    unsafe_copyto!(pointer(A), Lib.direct(handle.base), prod(handle.size))
    return A
end

# # Generic fallback
# function retrieve(x::Persistent{T}, ::IsOther) where {T}
#     offsets = fieldoffsets(x)
#     ptr = Lib.direct(getoid(x))
#     fields = []
# 
#     for index = 1:fieldcount(T)
#         _ptr = ptr + offsets[index]
#         _type = fieldtype(T, index)
#         trait = class(_type)
# 
#         field = _retrieve(convert(Ptr{_type}, _ptr), trait)
#         push!(fields, field)
#     end
#     return T(fields...)
# end

# _retrieve(ptr::Ptr, trait::IsBits) = retrieve(ptr, trait)
# function _retrieve(ptr::Ptr{T}, trait::AbstractClass) where {T} 
#     retrieve(convert(Ptr{aspersistent(T)}, ptr), trait)
# end

#####
##### Field Calculations
#####

_fieldnames(P::Persistent{T}) where {T} = fieldnames(T)

# For now, just tack on enough room for the last element to be a full pointer. Eventually
# we'll revisit this assumption
_allocsize(::Type{T}) where {T} = sizeof(PersistentOID{T}) + last(fieldoffsets(Persistent{T}))


# function _fieldoffsets(::Type{Persistent{T}}) where {T}
#     # Accumulate shifts we had to do to accomodate the extra room for persistent pointers
#     shift = 0
#     count = Tuple(1:fieldcount(T))
#     offsets = map(count) do index
#         _name = fieldname(T, index)
#         _type = fieldtype(T, index)
#         _offset = fieldoffset(T, index)
# 
#         if isbitstype(_type) || index == fieldcount(T)
#             return _offset + shift
#         else
#             # Get the size allocated for this field.
#             fieldsize = fieldoffset(T, index + 1) - _offset 
# 
#             # Compute the offset of this field, then compute how much we need to shift
#             # everything else below this.
#             thisoffset = _offset + shift
# 
#             shift += max(signed(sizeof(PersistentOID{T}) - fieldsize), 0)
#             return thisoffset
#         end
#     end
#     return NamedTuple{fieldnames(T)}(offsets)
# end
# 
# @generated function fieldoffsets(::Type{Persistent{T}}) where {T}
#     offsets = _fieldoffsets(Persistent{T})
#     return :($offsets)
# end
# fieldoffsets(P::Persistent{T}) where {T} = fieldoffsets(Persistent{T})
# 
# function getproperty(P::Persistent{T}, name::Symbol) where {T}
#     baseptr = Lib.direct(getoid(P))
#     offsets = fieldoffsets(P)
#     _type = fieldtype(T, name)
#     if isbitstype(_type)
#         ptr = convert(Ptr{_type}, baseptr)
#         return unsafe_load(ptr + offsets[name])
#     else
#         ptr = convert(Ptr{Persistent{_type}}, baseptr)
#         return unsafe_load(ptr + offsets[name])
#     end
# end

end
