using LibGit2

# Set to "true" to save the source download. Otherwise, remove the source and related
# object files after a successful build.
keep_source = false

# Download the source code
url = "https://github.com/pmem/pmdk/"
branch = "stable-1.5"
localdir = joinpath(@__DIR__, "pmdk")

# Cleanup leftovers
ispath(localdir) && rm(localdir; force = true, recursive = true)

LibGit2.clone(url, localdir; branch = branch)

# Navigate and build
olddir = pwd()
try
    cd(localdir)

    nprocs = parse(Int, read(`nproc`, String))
    run(`make -j$nprocs CC=clang CXX=clang++`)
    installdir = joinpath(@__DIR__, "usr")
    run(`make install prefix=$installdir`)

    # Delete cloned directory to cut down on accumulated size.
    keep_source || rm(localdir; force = true, recursive = true)
finally
    cd(olddir)
end
