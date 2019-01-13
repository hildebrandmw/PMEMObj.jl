module PersistentObjects

# exports
export ObjectPool, PersistentOID, transaction

# includes
include("lib.jl")
include("transaction.jl")
include("persistent.jl")

# include submodules
using .Lib
using .Transaction
using .Persistence

end # module
