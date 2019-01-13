@testset "Testing `pmemobj_open` class functions" begin
    using PersistentObjects.Lib

    testfile = joinpath(@__DIR__, "test.pool")
    ispath(testfile) && rm(testfile)

    # Just try some simple opening and closing.
    pool = Lib.create(testfile, 2^24)
    @test Lib.isnull(pool) == false

    Lib.close(pool)
    @test Lib.check(testfile) == true
    pool = Lib.open(testfile)
    @test Lib.isnull(pool) == false
    Lib.close(pool)

    # Since the file already exists, if we try to make another one with the same name,
    # we should get a null pool in return.
    pool = Lib.create(testfile, 2^24)
    @test Lib.isnull(pool) == true

    # Now, do some checks that the "layout" argument works
    rm(testfile) 
    pool = Lib.create(testfile, 2^24; layout = "layout_1")
    Lib.close(pool)

    pool = Lib.open(testfile; layout = "billy_bob")
    @test Lib.isnull(pool) == true

    pool = Lib.open(testfile; layout = "layout_1")
    @test Lib.isnull(pool) == false

    # Cleanup
    rm(testfile) 
end

@testset "Testing `pmemobj_root` class functions" begin
    using PersistentObjects.Lib

    testfile = joinpath(@__DIR__, "test.pool")
    ispath(testfile) && rm(testfile)

    pool = Lib.create(testfile, 2^24)
    # Allocate 4 Int64's for the root.
    oid = Lib.root(pool, 4 * sizeof(Int64), Int64)
    @test !Lib.isnull(oid)

    # Memory should be zeroed out - make sure this is the case using the scary unsafe loads.
    ptr = Lib.direct(oid)
    @test isa(ptr, Ptr{Int64})
    @test unsafe_load(ptr, 1) == 0
    @test unsafe_load(ptr, 2) == 0
    @test unsafe_load(ptr, 3) == 0
    @test unsafe_load(ptr, 4) == 0

    # Now do some unsafe stores - don't worry about performing transactions just yet.
    unsafe_store!(ptr, 1, 1)
    unsafe_store!(ptr, 2, 2)
    unsafe_store!(ptr, 3, 3)
    unsafe_store!(ptr, 4, 4)

    @test unsafe_load(ptr, 1) == 1
    @test unsafe_load(ptr, 2) == 2
    @test unsafe_load(ptr, 3) == 3
    @test unsafe_load(ptr, 4) == 4

    # Close the file and reopen it - make sure these values persist.
    Lib.close(pool) 
    pool = Lib.open(testfile)
    this_oid = Lib.root(pool, 4 * sizeof(Int64), Int64)

    # The persistent OID should be the same across loads
    @test this_oid == oid
    ptr = Lib.direct(this_oid)

    @test unsafe_load(ptr, 1) == 1
    @test unsafe_load(ptr, 2) == 2
    @test unsafe_load(ptr, 3) == 3
    @test unsafe_load(ptr, 4) == 4

    # Test resizing - make sure upper memory is zeroed
    oid = Lib.root(pool, 6 * sizeof(Int64), Int64) 
    ptr = Lib.direct(oid)
    @test unsafe_load(ptr, 1) == 1
    @test unsafe_load(ptr, 2) == 2
    @test unsafe_load(ptr, 3) == 3
    @test unsafe_load(ptr, 4) == 4
    @test unsafe_load(ptr, 5) == 0
    @test unsafe_load(ptr, 6) == 0

    # Cleanup
    Lib.close(pool) 
    rm(testfile) 
end

@testset "Testing `oid_is_null` class functions" begin
    using PersistentObjects.Lib

    testfile = joinpath(@__DIR__, "test.pool")
    ispath(testfile) && rm(testfile)

    pool = Lib.create(testfile, 2^24)

    # Create a root and make sure all the conversion functions work like we expect.
    root_oid = Lib.root(pool, sizeof(Int64))
    root_ptr = Lib.direct(root_oid)

    @test Lib.pool(root_oid) == pool
    @test Lib.pool(root_ptr) == pool

    @test Lib.oid(root_ptr) == root_oid

    # Cleanup
    Lib.close(pool) 
    rm(testfile)
end
