package = 'apex'
version = 'scm-1'
source  = {
	url    = 'git://github.com/moonlibs/apex.git',
	branch = 'reconfiguration_nodes',
}
description = {
	summary  = "Package for tarantool proxy - apex",
	homepage = 'https://github.com/moonlibs/apex.git',
	license  = 'BSD',
}
dependencies = {
	'lua >= 5.1',
	'val',
	'ctx',
	'obj',
	'sync'
}
build = {
	type = 'builtin',
	modules = {
		['apex.proxy']          = 'apex/proxy.lua';
		['apex.apex_instances'] = 'apex/apex_instances.lua';
		['apex.cluster']        = 'apex/cluster.lua';
		['apex.node']           = 'apex/node.lua';
		['apex.utils']          = 'apex/utils.lua';
	}
}

-- vim: syntax=lua
