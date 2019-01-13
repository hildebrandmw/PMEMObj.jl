module Persistence

# TODO: Implement Arrays
# TODO: Handle Circular References
# TODO: Cleanup loading and storing code
#   - Review existing constructs - there may be a simpler implementation
#   - Make generic persisting and storing type stable, either through code tricks or by
#       using @generated.
#
# TODO: write tests!

export persist, retrieve

import Base: getproperty, setproperty!

using ..Lib
using ..Transaction

abstract type AbstractClass end

struct IsBits <: AbstractClass end
struct IsString <: AbstractClass end
struct IsArray <: AbstractClass end
struct IsOther <: AbstractClass end

class(::Type{T}) where {T} = isbitstype(T) ? IsBits() : IsOther()
class(::Type{T}) where {T <: Union{String, Symbol}} = IsString()
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

#####
##### persist
#####

persist(pool, x::T) where {T} = persist(pool, x, class(T))

function persist(pool, x::T, trait::IsString) where {T <: Union{String, Symbol}}
    oid = transaction(() -> store(pool, x, IsString()), pool)
    return Persistent{T}(oid)
end

function persist(pool, x::T, trait::IsBits) where {T}
    oid = transaction(pool) do
        oid = Lib.tx_alloc(sizeof(x), T)
        ptr = Lib.direct(oid)
        _store(pool, ptr, x, trait)
        return oid
    end
    return Persistent{T}(oid)
end

# Generic fallback
#
# Only wrap the outermost `persist` in a transaction so inner calls to `_persist` don't
# nest transactions
persist(pool, x, trait::IsOther) = transaction(() -> _persist(pool, x, trait), pool)

function _persist(pool, x::T, trait::IsOther) where {T}
    size = _allocsize(T, trait) 
    oid = Lib.tx_alloc(size, T)
    ptr = Lib.direct(oid)
    
    # Copy over all the field elements of T
    offsets = fieldoffsets(Persistent{T}) 
    for index in 1:fieldcount(T)
        thisptr = ptr + offsets[index]
        object = getproperty(x, fieldname(T, index))

        _store(pool, thisptr, object, class(object))
    end
    return Persistent{T}(oid)
end

# Use this when composing fields of a composite type.
function _store(pool, ptr, x::T, trait::AbstractClass) where {T} 
    object = store(pool, x, trait)

    # For debugging purposes. Objects returned by store must always be isbits
    @assert isbits(object)
    unsafe_store!(convert(Ptr{typeof(object)}, ptr), object)
end

# Storage methods
store(pool, x::T, ::IsString) where {T <: Union{String,Symbol}} = Lib.tx_strdup(String(x), T)
store(pool, x, ::IsBits) = x
store(pool, x, trait::IsOther) = _persist(pool, x, trait)

#####
##### retrieve
#####

retrieve(x::Persistent{T}) where {T} = retrieve(x, class(T))
retrieve(x::Persistent, trait::AbstractClass) = retrieve(Lib.direct(getoid(x)), trait)

# Specializtions
retrieve(ptr::Ptr{T}, ::IsString) where {T} = T(unsafe_string(convert(Ptr{UInt8}, ptr)))
retrieve(ptr::Ptr, ::IsBits) = unsafe_load(ptr)

# Generic fallback
function retrieve(x::Persistent{T}, ::IsOther) where {T}
    offsets = fieldoffsets(x)
    ptr = Lib.direct(getoid(x))
    fields = []

    for index = 1:fieldcount(T)
        _ptr = ptr + offsets[index]
        _type = fieldtype(T, index)
        trait = class(_type)

        field = _retrieve(convert(Ptr{_type}, _ptr), trait)
        push!(fields, field)
    end
    return T(fields...)
end

_retrieve(ptr::Ptr, trait::IsBits) = retrieve(ptr, trait)
_retrieve(ptr::Ptr{T}, trait::AbstractClass) where {T} = retrieve(unsafe_load(convert(Ptr{Persistent{T}}, ptr)))

#####
##### Field Calculations
#####

_fieldnames(P::Persistent{T}) where {T} = fieldnames(T)

# For now, just tack on enough room for the last element to be a full pointer. Eventually
# we'll revisit this assumption
_allocsize(::Type{T}, ::IsOther) where {T} = sizeof(PersistentOID{T}) + last(fieldoffsets(Persistent{T}))


function _fieldoffsets(::Type{Persistent{T}}) where {T}
    # Accumulate shifts we had to do to accomodate the extra room for persistent pointers
    shift = 0
    count = Tuple(1:fieldcount(T))
    offsets = map(count) do index
        _name = fieldname(T, index)
        _type = fieldtype(T, index)
        _offset = fieldoffset(T, index)

        if isbitstype(_type) || index == fieldcount(T)
            return _offset + shift
        else
            # Get the size allocated for this field.
            fieldsize = fieldoffset(T, index + 1) - _offset 

            # Compute the offset of this field, then compute how much we need to shift
            # everything else below this.
            thisoffset = _offset + shift

            shift += max(signed(sizeof(PersistentOID{T}) - fieldsize), 0)
            return thisoffset
        end
    end
    return NamedTuple{fieldnames(T)}(offsets)
end

@generated function fieldoffsets(::Type{Persistent{T}}) where {T}
    offsets = _fieldoffsets(Persistent{T})
    return :($offsets)
end
fieldoffsets(P::Persistent{T}) where {T} = fieldoffsets(Persistent{T})

function getproperty(P::Persistent{T}, name::Symbol) where {T}
    baseptr = Lib.direct(getoid(P))
    offsets = fieldoffsets(P)
    _type = fieldtype(T, name)
    if isbitstype(_type)
        ptr = convert(Ptr{_type}, baseptr)
        return unsafe_load(ptr + offsets[name])
    else
        ptr = convert(Ptr{Persistent{_type}}, baseptr)
        return unsafe_load(ptr + offsets[name])
    end
end

end
