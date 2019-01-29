module Persistence

# exports
export ObjectPool, PersistentOID, transaction

# includes
include("lib.jl")
include("transaction.jl")
include("persist.jl")
include("trace.jl")
include("arrays.jl")

# include submodules
using .Lib
using .Transaction
using .Persist
using .Arrays

end # module
