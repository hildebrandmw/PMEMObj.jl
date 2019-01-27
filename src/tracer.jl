module Tracer

using Cassette

Cassette.@context ArrayTraceCtx

_track(ctx::ArrayTraceCtx) = nothing
_track(ctx::ArrayTraceCtx, x, args...) = _track(ctx, args...)
function _track(ctx::ArrayTraceCtx, x::AbstractArray, args...)
    haskey(ctx.metadata, x) && (ctx.metadata[x] = size(x))
    _track(ctx, args...)
end

# Prehook everythig, grab all references to arrays
prehook(ctx::ArrayTraceCtx, f, args) = track(ctx, args...)

function track(f, args)
    metadata = IdDict()
    ctx = ArrayTraceCtx(metadata = metadata)
    overdub(ctx, f, args)
    return metadata
end

end
