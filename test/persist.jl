struct IsBitsStruct
    a::Int32
    b::Int64
    c::Float32
end

mutable struct MutableBitsStruct
    a::Int32
    b::Int32
    c::Int64
end


@testset "Testing Persist" begin
    import PersistentObjects.Lib
    using PersistentObjects.Persist

    poolfile = "test.pool"  
    ispath(poolfile) && rm(poolfile)

    pool = Lib.create(poolfile, 2^24)

    #####
    ##### isbits testing
    #####

    # int64
    A = 1
    p = persist(pool, A)
    @test isa(p, Persistent{Int})
    @test Lib.isnull(Persist.getoid(p)) == false
    B = retrieve(p)
    @test A === B

    # float32
    A = Float32(10)
    p = persist(pool, A)
    @test isa(p, Persistent{Float32})
    @test Lib.isnull(Persist.getoid(p)) == false
    B = retrieve(p)
    @test A === B

    # isbits tuple
    A = (1, 1.0, UInt8(5), Float32(-1.0), Int16(-100))
    p = persist(pool, A)
    @test Lib.isnull(Persist.getoid(p)) == false
    B = retrieve(p)
    @test A === B

    # custom `isbits` struct
    @test isbitstype(IsBitsStruct) 
    A = IsBitsStruct(typemin(Int32), typemax(Int64), -0.0)
    p = persist(pool, A)
    B = retrieve(p)
    @test A === B

    #####
    ##### string testing
    #####
    
    # String
    A = "hello world"
    p = persist(pool, A)
    B = retrieve(p)
    @test A == B

    # Symbol
    A = :symbol
    p = persist(pool, A)
    B = retrieve(p)
    @test typeof(A) == typeof(B)
    @test A === B

    #####
    ##### array testing
    #####
    

end
