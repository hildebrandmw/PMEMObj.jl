module PersistentObjects

# exports
export ObjectPool, PersistentOID, transaction

# includes
include("lib.jl")
include("transaction.jl")
include("persist.jl")

# include submodules
using .Lib
using .Transaction
using .Persist

end # module
