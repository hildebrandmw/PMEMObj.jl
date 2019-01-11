module Types

using ..Lib
using ..Transaction

import Base.Iterators: drop
#####
##### Support for storing type information in persistent memory
#####

const TAGS = Any[
    Symbol,
    Nothing,
    UInt128
]

_tag(@nospecialize(::Type{T})) where {T} = UInt8(findfirst(isequal(T), TAGS))

const SYMBOL_TAG = _tag(Symbol)
const NOTHING_TAG = _tag(Nothing)
const UINT128_TAG = _tag(UInt128)

# The idea here is to convert the symbol to a string, store the string, and create a 
# TypeTag with a reference to the string. We then store the type tag in persistent memory
# and return the PersistentOID to the type tag.
#
# !!! This should only be called inside a transaction.
function _tx_store(x::Symbol)
    # Perform a transaction to store this symbol and create the type tag. Return the
    # type tag.

    # Atomimcally copy the string name for the symbol to persistent memory
    pname = unsafe_convert(Ptr{UInt8}, x)
    string_oid = Lib.tx_strdup(pname)

    return oid
end

struct ModuleLayout
    hasuuid::Bool
    uuid::Int128  
    numnames::UInt8
    # names
end

# The names hang off the end of the module layout and are persistent pointers to
# symbols.
totalsize(m::ModuleLayout) = sizeof(m) + sizeof(PersistentOID{Nothing}) * m.numnames

function _tx_load_symbol(oid)
end

function _tx_load_module(oid::PersistentOID{ModuleLayout})
    ptr = Lib.direct(oid)       
    layout = unsafe_load(ptr)

    # Load the name symbols
    names = Vector{PersistentOid{Symbol}}(undef, layout.numnames)
    src_pointer = convert(Ptr{PersistentOid{Symbol}}, ptr + sizeof(ModuleLayout))
    unsafe_copyto!(pointer(names), src_pointer, length(names))

    # Load the modules now
    name = first(names)  
    pkg = layout.hasuuid ? Base.PkgID(Base.UUID(layout.uuid), name) : Base.PkgID(name)

    m = Base.root_module(pkg)
    for index in 2:length(names)
        m = getfield(m, names[index])::Module
    end
    return m
end




end
