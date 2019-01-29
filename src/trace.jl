module Tracer

using Cassette
import Cassette: overdub, prehook

Cassette.@context ArrayTraceCtx

_track(ctx::ArrayTraceCtx) = nothing
_track(ctx::ArrayTraceCtx, x, args...) = _track(ctx, args...)
function _track(ctx::ArrayTraceCtx, x::Array, args...)
    get!(ctx.metadata, objectid(x), length(x))
    _track(ctx, args...)
end

# Prehook everythig, grab all references to arrays
prehook(ctx::ArrayTraceCtx, f, args...) = _track(ctx, args...)

function track(f, args...)
    metadata = IdDict{UInt64, Int64}()
    ctx = ArrayTraceCtx(metadata = metadata)
    overdub(ctx, f, args...)
    return ctx.metadata
end

end
