using Documenter, Persistence

makedocs(
    modules = [Persistence],
    format = :html,
    sitename = "Persistence.jl",
    pages = Any["index.md"]
)

deploydocs(
    repo = "github.com/hildebrandmw/Persistence.jl.git",
    target = "build",
    julia = "1.0",
    deps = nothing,
    make = nothing,
)
