module Transaction

using ..Lib

export transaction

# This is taken from the C++ implementation:
#
# https://github.com/pmem/libpmemobj-cpp/blob/master/include/libpmemobj%2B%2B/transaction.hppF:w
#
# Pass a closure to the transaction for a given pool pointer. Should automatically deal with
# cleaning up and rolling back
function transaction(f, pool)
    # Start transaction
    Lib.tx_begin(pool)

    local obj
    try
        obj = f()
    catch err
        # Error was thrown while executing.
        # Need to call `_tx_abort` and then throw the exception up the stack
        if Lib.tx_stage() == Lib.TX_STAGE_WORK
            Lib.tx_abort(-1)
        end
        Lib.tx_end()
        throw(err)
    end

    # Handle cleanup
    stage = Lib.tx_stage()

    # Optimistic case - everything went well, just need to commit and end
    if stage == Lib.TX_STAGE_WORK
        Lib.tx_commit()

    # Abort was called in "f" 
    elseif stage == Lib.TX_STAGE_ONABORT
        Lib.tx_end()
        throw(error("transaction aborted"))

    # "tx_end" was called in "f"
    elseif stage == Lib.TX_STAGE_NONE
        throw(error("transaction ended prematurely"))
    end

    Lib.tx_end()
    return obj
end

end # module
