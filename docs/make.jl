using Documenter, PMemObj

makedocs(
    modules = [PMemObj],
    format = :html,
    sitename = "PMemObj.jl",
    pages = Any["index.md"]
)

deploydocs(
    repo = "github.com/hildebrandmw/PMemObj.jl.git",
    target = "build",
    julia = "1.0",
    deps = nothing,
    make = nothing,
)
