package = 'apex'
version = 'scm-1'
source  = {
    url    = 'git://github.com/moonlibs/apex.git',
    branch = 'master',
}
description = {
    summary  = "Balancer proxy for tarantool ",
    homepage = 'https://github.com/moonlibs/apex.git',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',
    modules = {
        ['apex.node'] = 'apex/node.lua';
        ['apex.proxy'] = 'apex/proxy.lua';
        ['apex.cluster'] = 'apex/cluster.lua';
    }
}

-- vim: syntax=lua
