local obj = require 'obj'
local log = require 'log'
local fiber = require 'fiber'
local sync = require 'sync'
local clock = require 'clock'

local DEFAULT_TIMEOUT = 1.5

-- local caller = require 'devel.caller'
local function caller() return '' end

local M = obj.class({},'apex.proxy')
local mt = debug.getmetatable(M)

local function sort_by(t,field)
	table.sort(t,function(a,b)
		return a[field] < b[field]
	end)
end

local function table_find_remove(where,what)
	if not where then return end
	for i,v in pairs(where) do
		if v == what then
			table.remove(where,i)
			break
		end
	end
end

function M:_init(t)
	local upstream = t.upstream
	if not upstream then error("app.upstream config required",2) end
	self.timeout = t.timeout or DEFAULT_TIMEOUT
	self.waiting_timeout = t.waiting_timeout or self.timeout
	self.on_error = t.on_error
	self.mode = t.mode or 'rw'
	
	local peers = {}
	local clusters = {}
	local default

	if upstream.etcd then
		assert(upstream.etcd.prefix,"app.upstream.etcd.prefix required")
		local cluster_set = config.etcd:list(upstream.etcd.prefix.."/clusters")
		
		local cluster, cluster_name

		if upstream.etcd.cluster then
			cluster = cluster_set[ upstream.etcd.cluster ]
			if not cluster then
				error("Not found cluster "..(upstream.etcd.cluster).." in etcd",0)
			end
			cluster_name = upstream.etcd.cluster
		else
			local cnt = 0
			for k,v in pairs(cluster_set) do
				cnt = cnt + 1
			end
			if cnt > 1 then
				error("Multiple clusters detected. not implemented yet. set app.upstream.etcd.cluster",0)
			end
			cluster_name,cluster = next(cluster_set)
			print("take ",cluster_name,cluster,"as default")
		end
		clusters[cluster_name] = require'apex.cluster'{ name = cluster_name }
		default = clusters[cluster_name]
		-- print(yaml.encode({ cluster_set, cluster_name, cluster_set }))
		
		local instance_set = config.etcd:list(upstream.etcd.prefix.."/instances")
		-- print(yaml.encode(instance_set))
		for name, conf in pairs(instance_set) do
			if conf.cluster == cluster_name then
				local addr = conf.box.remote_addr or conf.box.listen
				if not addr then error("No address info in "..name) end
				table.insert(peers,{
					name = name;
					addr = addr;
					cluster = cluster_name;
				})
			end
		end
		if #peers == 0 then error("Peeked no instances by current config",0) end
	else
		error("Misconfig",0)
	end

	for _,peer in pairs(peers) do
		local node = require'apex.node'{
			name    = peer.name;
			addr    = peer.addr;
			cluster = peer.cluster;
		}
		node:on('state', function(node,event,state,old)
			-- stat:send(string.format("state.%s!%s",old, state),1)
			print(string.format("state.%s!%s", old, state))
		end)
		
		local cluster = clusters[node.cluster]
		clusters[node.cluster] = cluster
		cluster:add(node)
	end
	
	self.clusters = clusters
	self.peers = peers
	self.default = default
	
end

function M:connect(cv)
	local start_cv = sync.cv();
	self.default:connect(start_cv)
	start_cv:wait()
end

function M:default_log(t)
	log.info(
		"[%s]\t[END=%s]\t%s:%s\tT=%0.3fs\t%s\t%s",
		t.request_id or '-',
		t.status, t.mode, t.func, t.run,
		t.node and t.node:describe() or '-',
		t.debug or ''
	)
end

function M:autoload(namespace, params)
	local cf = {}
	cf.error_status  = params.error_status
	
	cf.prefix  = params.prefix or ''
	cf.timeout = params.timeout or self.timeout
	cf.mode    = params.mode or self.mode or 'rw'
	
	
	cf.on_call    = params.on_call or self.on_call
	cf.request_id = params.request_id or self.request_id
	cf.on_error   = params.on_error or self.on_error
	cf.need_retry = params.need_retry or self.need_retry
	
	cf.log = params.log or self.log or function(...) self:default_log(...) end

	local auto_mt;
	auto_mt = {
		__self  = self;
		__cf    = cf;
		
		__index = function(t,name)
			local parent = rawget(t,"name")
			local nest = {
				name = (parent and parent .. '.' or '') .. name;
			}
			rawset(t,name,setmetatable(nest,auto_mt))
			return t[name];
		end;
		__call  = function(t,...)
			local name = assert(rawget(t,"name"),"Call to unnamed")
			local mt = getmetatable(t)
			return mt.__self:wrapped(mt.__cf, name, ...)
		end;
	}
	
	return setmetatable(namespace,auto_mt)
end

function M:wrap(fun)
	return function(...)
		fun(...)
	end
end

function M:peer(mode)
	local cluster = self.default
	mode = mode or self.mode
	assert(cluster[mode],"Unknown mode for cluster "..mode)
	local node = cluster[mode]( cluster, self.waiting_timeout )
	return node
end

local function on_call(ctx,name,...)
	return name,ctx.mode,...
end

local function do_call(self,cf,ctx,func,mode,...)
	print(cf,ctx,func,mode,...)
	ctx.call = cf.prefix .. func;
	ctx.mode = mode or 'rw';
	ctx.request_id = cf.request_id and cf.request_id(ctx,...)
	local cluster = self.default
	local modes = type(ctx.mode) == 'table' and ctx.mode or {ctx.mode}
	local tries = #modes
	for i,mode in pairs(modes) do
		local node
		local debug
		local start = clock.time()
		if not cluster[mode] then
			debug = "Unknown mode for cluster "..mode
		else
			print("Select node for "..mode)
			node = cluster[mode]( cluster, self.waiting_timeout )
		end
		ctx.retry  = nil
		ctx.status = nil
		ctx.debug  = nil
		ctx.node = node
		ctx.mode = mode
		if not node then
			ctx.run = clock.time()-start
			ctx.status = cf.error_status or "ERR"
			ctx.debug = ctx.debug or "No peers available";
			cf.log(ctx)
			if i == tries then
				return cf.on_error and cf.on_error(ctx)
					or error(ctx.debug,4)
			else
				if cf.on_error then
					local ret = {cf.on_error(ctx)}
					if ctx.retry == false then
						return unpack(ret)
					end
				end
			end
		else
			local r,result = pcall(function(node,timeout,...)
				return node.conn:timeout(timeout):call(...)
			end, node, ctx.timeout, func, ...)
			ctx.run = clock.time()-start
			
			if r then
				if cf.on_response then
					result = cf.on_response(ctx, result)
				end
				ctx.status = ctx.status or cf.success_status or "OK"
				ctx.response = result[1]
				cf.log(ctx)
				if ctx.retry ~= nil then
					if not ctx.retry or i == tries then
						return unpack(result)
					end
				elseif cf.need_retry then
					if not cf.need_retry(ctx,result) or i == tries then
						return unpack(result)
					end
				end
			else
				ctx.debug = result;
				ctx.status = cf.error_status or "ERR";
				cf.log(ctx)
				
				if i == tries then
					return cf.on_error and cf.on_error(ctx)
						or error(ctx.debug,4)
				else
					if cf.on_error then cf.on_error(ctx) end
				end
			end
		end
	end
end

function M:wrapped(cf, name, ...)
	print("wrap",name,...)
	local ctx = {
		func = name;
		mode = cf.mode or 'rw';
		timeout = cf.timeout or self.timeout;
	}
	do
	return do_call(self,cf,ctx,(cf.on_call or on_call)(ctx,...))
	end
	
	local request_id = cf.request_id and cf.request_id(name,...)
	local func,mode
	if cf.map then
		func,mode = cf.prefix..cf.map(name)
	else
		func = cf.prefix..name
	end
	mode = mode or cf.mode or 'rw'
	
	local cluster = self.default

	assert(cluster[mode],"Unknown mode for cluster "..mode)
	local node = cluster[mode]( cluster, self.waiting_timeout )

	local start = clock.time()
	
	if not node then
		local run = clock.time()-start
		local res = {
			request_id = request_id;
			run = run;
			func = func;
			mode = mode;
			status = "ERR";
			error = "no peers available";
			debug = "no peers available";
		}
		cf.log(res)
		return cf.on_error and cf.on_error(res)
			or error("No peers available",3)
		-- self:default_error_unavailable(res)
	end

	local r,info = pcall(function(node,timeout,...)
		return node.conn:timeout(timeout):call(...)
	end, node, cf.timeout, func, ...)
	
	local run = clock.time()-start
	print("run = ",run, "r = ",r, info)
	if r then
		if #info > 0 then
			local res = info[1]
			local log = cf.unbox(res)
			log.request_id = request_id;
			log.run = run;
			log.func = func;
			log.mode = mode;
			log.node = node;
			log.response = res;
			cf.log(log)
			-- stat:send(string.format(stat_name,res[1]),run)
			-- log.info("[%s]\t<%s>\t[END=%s]\tT=%0.3fs\t%s\t%s", x_req_id, args.bucket or "-", res[1], run, func, cluster.name)
			return res
		else
			cf.log{
				request_id = request_id;
				run = run;
				func = func;
				mode = mode;
				node = node;
				status = "OK";
				debug = "empty response";
			}
			return
		end
	else
		local res = {
			request_id = request_id;
			run = run;
			func = func;
			status = "ERR";
			debug = info;
		}
		cf.log(res)
		return cf.on_error and cf.on_error(res)
			or error(info,3)
	end
end

return M