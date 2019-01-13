using Documenter, PersistentObjects

makedocs(
    modules = [PersistentObjects],
    format = :html,
    sitename = "PersistentObjects.jl",
    pages = Any["index.md"]
)

deploydocs(
    repo = "github.com/hildebrandmw/PersistentObjects.jl.git",
    target = "build",
    julia = "1.0",
    deps = nothing,
    make = nothing,
)
