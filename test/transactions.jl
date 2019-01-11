struct DummyError <: Exception end

function _setup_transaction_pool(size, file)
    ispath(file) && rm(file)

    pool = Lib.create(file, 2^24)

    # The root is a pointer to an array that we will dynamically allocate.
    roottype = PersistentOID{Int64}
    rootoid = Lib.root(pool, sizeof(roottype), roottype)

    # Do a transaction to allocate the array.
    transaction(pool) do
        oid = Lib.tx_zalloc(size, Int64)

        Lib.add_range(rootoid, 0, sizeof(roottype)) 
        rootptr = Lib.direct(rootoid)
        unsafe_store!(rootptr, oid)
    end

    return pool
end

@testset "Testing Transactions" begin
    using PMemObj.Lib
    using PMemObj.Transaction
    # The best way to test transactions is probably through the "transaction" function.
    # 
    # The general approach is to have a root that is pointing to an array some where, and
    # try various ways of screwing up the transaction and making sure everything is in a
    # consistent state.
     
    testfile = "test.pool"
    roottype = PersistentOID{Int64}

    # Set up some ground truth arrays.
    testlength = 100000
    A = collect(1:testlength)
    B = collect(testlength:-1:1)

    @testset "Testing Normal Transactions" begin
        pool = _setup_transaction_pool(sizeof(A), testfile)

        transaction(pool) do
            # Get the persistent oid to the array
            rootoid = Lib.root(pool, sizeof(roottype), roottype)
            rootptr = Lib.direct(rootoid)

            oid = unsafe_load(rootptr)

            # Mark the memory for the array for transaction and copy A
            Lib.add_range(oid, 0, sizeof(A))  
            ptr = Lib.direct(oid)
            unsafe_copyto!(ptr, pointer(A), length(A))
        end

        Lib.close(pool)  
        pool = Lib.open(testfile)

        rootoid = Lib.root(pool, sizeof(roottype), roottype)
        rootptr = Lib.direct(rootoid)

        # Load the persistent pointer from the root.
        # Convert the persistent pointer to a normal pointer and load the stored array
        oid = unsafe_load(rootptr)  
        ptr = Lib.direct(oid)

        dest = zeros(Int64, testlength)
        unsafe_copyto!(pointer(dest), ptr, testlength)

        @test dest == A

        # Cleanup
        Lib.close(pool) 
    end

    @testset "Testing Aborted Transaction" begin
        # Manually call "abort" during the loading of A
        pool = _setup_transaction_pool(sizeof(A), testfile)

        f() = transaction(pool) do
            # Get the persistent oid to the array
            rootoid = Lib.root(pool, sizeof(roottype), roottype)
            rootptr = Lib.direct(rootoid)

            oid = unsafe_load(rootptr)

            # Mark the memory for the array for transaction and copy A
            Lib.add_range(oid, 0, sizeof(A))  
            ptr = Lib.direct(oid)
            unsafe_copyto!(ptr, pointer(A), length(A))

            # Now, everything is copied directly, call "abort"
            Lib.tx_abort(10) 
        end
        @test_throws TransactionAborted f()

        Lib.close(pool)  
        pool = Lib.open(testfile)

        rootoid = Lib.root(pool, sizeof(roottype), roottype)
        rootptr = Lib.direct(rootoid)

        ## Load the persistent pointer from the root.
        ## Convert the persistent pointer to a normal pointer and load the stored array
        oid = unsafe_load(rootptr)  
        ptr = Lib.direct(oid)

        dest = zeros(Int64, testlength)
        unsafe_copyto!(pointer(dest), ptr, testlength)

        @test dest == zeros(Int64, testlength)

        ## Cleanup
        Lib.close(pool) 
    end

    @testset "Testing Errored Transactions" begin
        # Manually call "abort" during the loading of A
        pool = _setup_transaction_pool(sizeof(A), testfile)

        f() = transaction(pool) do
            # Get the persistent oid to the array
            rootoid = Lib.root(pool, sizeof(roottype), roottype)
            rootptr = Lib.direct(rootoid)

            oid = unsafe_load(rootptr)

            # Mark the memory for the array for transaction and copy A
            Lib.add_range(oid, 0, sizeof(A))  
            ptr = Lib.direct(oid)
            unsafe_copyto!(ptr, pointer(A), length(A))

            # Throw dummy exception
            throw(DummyError()) 
        end
        @test_throws DummyError f()

        Lib.close(pool)  
        pool = Lib.open(testfile)

        rootoid = Lib.root(pool, sizeof(roottype), roottype)
        rootptr = Lib.direct(rootoid)

        ## Load the persistent pointer from the root.
        ## Convert the persistent pointer to a normal pointer and load the stored array
        oid = unsafe_load(rootptr)  
        ptr = Lib.direct(oid)

        dest = zeros(Int64, testlength)
        unsafe_copyto!(pointer(dest), ptr, testlength)

        @test dest == zeros(Int64, testlength)

        ## Cleanup
        Lib.close(pool) 
    end
end
