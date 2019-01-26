using Persistence
using Persistence.Persist
import Persistence.Lib

using BenchmarkTools

function transact(P::PersistentArray, values)
    transaction(P) do
        _copy(P, values)
    end
end

nontransact(P::PersistentArray, values) = _copy(P, values)

_copy(P::PersistentArray, values) = unsafe_copyto!(P.base, pointer(values), length(values))

function test(size, iters = 100000)
    file = "/mnt/test.pool"
    ispath(file) && rm(file)
    pool = Lib.create(file, 2^30)

    P = PersistentArray{Int64}(pool, (size,))
    values = rand(Int64, size)
    for _ in 1:iters
        _copy(P, values)
    end
end

function benchmark()
    sizes = [10 ^ i for i in 3:7]

    poolfile = "/mnt/test.pool"

    for size in sizes
        try
            println("Working with file $poolfile")
            ispath(poolfile) && rm(poolfile)
            pool = Lib.create(poolfile, 2^30)

            P = PersistentArray{Int64}(pool, (size,))
            indices = 1:size
            values = rand(Int64, size)
            
            results = @benchmark nontransact($P, $indices, $values)

            println("Results for size: $size")
            display(results)

            Lib.close(pool)
        finally
            rm(poolfile)
        end
    end
end
