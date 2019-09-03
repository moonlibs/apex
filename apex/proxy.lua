local obj = require 'obj'
local json = require 'json'
local log = require 'log'
local fiber = require 'fiber'
local sync = require 'sync'
local clock = require 'clock'
local val = require 'val'
local ctx_t = require 'ctx'
local utils = require 'apex.utils'

-- local caller = require 'devel.caller'
local function caller() return '' end

local M = obj.class({},'apex.proxy')
local mt = debug.getmetatable(M)

local WAITING_TIMEOUT = 1.5

local proxy_validation_scheme = {
	upstream = val.req("table", {
		etcd = val.req("table", {
			prefix = "+string";
		});
	});
	nodes_conf = val.opt("table", {
		timeout            = val.opt(val.num);
		connect_timeout    = val.opt(val.num);
		reconnect_interval = val.opt(val.num);
		ping_interval      = val.opt(val.num);
		ping_timeout       = val.opt(val.num);
		fail_max_count     = val.opt(val.num);
	});
	waiting_timeout = val.opt(val.num);
}

local init_val = val.idator({
	proxy = val.req("table", proxy_validation_scheme);
	etcd  = val.req("table", {
		prefix = "+string";
	});
})
function M:_init(args)
	init_val(args)

	self.ctx = ctx_t("proxy")
	self.ctx.log.store = nil

	self.etcd = { prefix = args.etcd.prefix }
	self.upstream = args.proxy.upstream
	self.waiting_timeout = args.proxy.waiting_timeout or WAITING_TIMEOUT

	local params = self:_get_cluster_params(args.proxy)
	self.default = require 'apex.cluster'(params)
	self.ctx.log:info('take cluster %s as default', self.default.name)

	self.apexes  = require 'apex.apex_instances'({ etcd = args.etcd })
	self.ctx.log:info('configure connect to other apex instances')
end

function M:_get_cluster_params(args)
	local cluster_set = config.etcd:list(args.upstream.etcd.prefix.."/clusters")
	local cluster_name
	if args.upstream.etcd.cluster then
		if not cluster_set[ args.upstream.etcd.cluster ] then
			error("not found cluster " .. args.upstream.etcd.cluster .. " in etcd", 2)
		end
		cluster_name = args.upstream.etcd.cluster
	else
		local cnt = 0
		for name, cluster in pairs(cluster_set) do
			cluster_name = name
			cnt = cnt + 1
			if cnt > 1 then
				error("multiple clusters detected. not implemented yet. set self.upstream.etcd.cluster", 2)
			end
		end
	end
	return {
		name = cluster_name;
		upstream = args.upstream;
		nodes_conf = args.nodes_conf;
	}
end

local proxy_val = val.idator(proxy_validation_scheme)
function M:reconfigure(args)
	if not args then args = {} end
	self.ctx.log:info("start reconfigure current apex")
	local app = config.etcd:list(self.etcd.prefix.."/common/app/proxy")
	proxy_val(app)

	self.waiting_timeout = app.waiting_timeout or WAITING_TIMEOUT
	local params = self:_get_cluster_params(app)
	if params.name ~= self.default.name then
		error(string.format("impossible to change cluster. Try to change %s -> %s",
			self.upstream.etcd.prefix, app.upstream.etcd.prefix), 2)
	end
	local start_cv = sync.cv()
	self.default:update(params, start_cv)
	start_cv:wait()

	local current_nodes_state = self.default:nodes_state()
	self.ctx.log:info("finish reconfigure current apex")
	if not args.reconfigure_all then
		return box.tuple.new({200, current_nodes_state})
	end

	self.ctx.log:info("start reconfigure other apexes")
	local info = {}
	local current_inst_name = config.get('sys.instance_name')
	info[current_inst_name] = {
		current_node = true;
		status = 'success';
	}
	local res = self.apexes:call('proxy:reconfigure')
	self.ctx.log:info("get reconfigure result: %s", json.encode(res))
	for name, struct in pairs(res) do
		info[name] = {
			status = 'failed';
			message = json.encode(struct);
		}
		if (type(struct) == 'cdata' or type(struct) == 'table')
			and type(struct[1]) == 'number' and struct[1] == 200
			and type(struct[2]) == 'table'
			and utils.compare_deeply(struct[2], current_nodes_state)
		then
			info[name].status = 'success'
			info[name].message = nil
		end
	end
	self.ctx.log:info("finish reconfigure other apexes")
	return {
		instances = info;
		nodes_state = current_nodes_state;
	}
end

function M:connect()
	local start_cv = sync.cv()
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

local autoload_val = val.idator({
	error_status   = "+string";
	success_status = "+string";
	prefix         = "+string";
	timeout        = val.opt(val.num);
	mode           = "+string";
	on_call        = "+function";
	on_response    = "+function";
	on_error       = "+function";
	need_retry     = "?function";
	log            = "+function";
	request_id     = "+function";
})
function M:autoload(namespace, params)
	autoload_val(params)
	local cf = params

	local auto_mt
	auto_mt = {
		__self  = self;
		__cf    = cf;
		
		__index = function(t,name)
			local parent = rawget(t,"name")
			local nest = {
				name = (parent and parent .. '.' or '') .. name;
			}
			rawset(t, name, setmetatable(nest, auto_mt))
			return t[name]
		end;
		__call  = function(t,...)
			local name = assert(rawget(t, "name"), "Call to unnamed")
			local mt = getmetatable(t)
			return mt.__self:wrapped(mt.__cf, name, ...)
		end;
	}

	return setmetatable(namespace, auto_mt)
end

function M:wrap(fun)
	return function(...)
		fun(...)
	end
end

-- function M:peer(mode)
-- 	local cluster = self.default
-- 	mode = mode or self.mode
-- 	assert(cluster[mode],"Unknown mode for cluster "..mode)
-- 	local node = cluster[mode]( cluster, self.waiting_timeout )
-- 	return node
-- end

local function on_call(ctx,name,...)
	return name, ctx.mode, ...
end

local LOG_MT = {
	__index = {
		info = function(self, str, ...)
			log.info("[%s]\t%s", self.reqid, tostring(str):format(...))
		end;

		warn = function(self, str, ...)
			log.warn("[%s]\t%s", self.reqid, tostring(str):format(...))
		end;

		error = function(self, str, ...)
			log.error("[%s]\t%s", self.reqid, tostring(str):format(...))
		end;
	}
}

local function build_log(reqid)
	return setmetatable({ reqid = tostring(reqid) }, LOG_MT)
end

local function do_call(self, cf, ctx, func, mode, ...)
	ctx.call = cf.prefix .. func
	ctx.mode = mode or 'rw'
	local cluster = self.default

	local modes = type(ctx.mode) == 'table' and ctx.mode or {ctx.mode}
	local tries = #modes

	for i, mode in pairs(modes) do
		local node
		local debug
		local start = clock.time()
		if not cluster[mode] then
			debug = "Unknown mode for cluster "..mode
		else
			node = cluster[mode]( cluster, self.waiting_timeout )
		end
		ctx.retry  = nil
		ctx.status = nil
		ctx.debug  = nil
		ctx.node = node
		ctx.mode = mode
		if not node then
			ctx.run = clock.time() - start
			ctx.status = cf.error_status or "ERR"
			ctx.debug = ctx.debug or "No peers available"
			cf.log(ctx)
			if i == tries then
				return cf.on_error and cf.on_error(ctx)
					or error(ctx.debug, 4)
			else
				if cf.on_error then
					local ret = {cf.on_error(ctx)}
					if ctx.retry == false then
						return unpack(ret)
					end
				end
			end
		else
			local conn_timeout = ctx.timeout or node.timeout
			local r,result = pcall(function(node, timeout, ...)
				return node.conn:timeout(timeout):call(...)
			end, node, conn_timeout, ctx.call, ...)
			ctx.run = clock.time() - start

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
					if not cf.need_retry(ctx, result) or i == tries then
						return unpack(result)
					end
				else
					return unpack(result)
				end
			else
				ctx.debug = result
				ctx.status = cf.error_status or "ERR"
				cf.log(ctx)

				if i == tries then
					return cf.on_error and cf.on_error(ctx)
						or error(ctx.debug, 4)
				else
					if cf.on_error then cf.on_error(ctx) end
				end
			end
		end
	end
end

function M:wrapped(cf, name, ...)
	-- print("wrap",name,...)
	local ctx = {
		func = name;
		mode = cf.mode;
		timeout = cf.timeout;
	}
	ctx.request_id = cf.request_id and cf.request_id(ctx,...)
	ctx.log = build_log(ctx.request_id)
	do
	return do_call(self, cf, ctx, (cf.on_call or on_call)(ctx,...))
	end

	local request_id = cf.request_id and cf.request_id(name,...)
	local func,mode
	if cf.map then
		func,mode = cf.prefix..cf.map(name)
	else
		func = cf.prefix..name
	end
	mode = mode or cf.mode

	local cluster = self.default

	assert(cluster[mode], "Unknown mode for cluster "..mode)
	local node = cluster[mode]( cluster, self.waiting_timeout )

	local start = clock.time()

	if not node then
		local run = clock.time() - start
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
	-- print("run = ", run, "r = ", r, info)
	if r then
		if #info > 0 then
			local res = info[1]
			local log = cf.unbox(res)
			log.request_id = request_id
			log.run = run
			log.func = func
			log.mode = mode
			log.node = node
			log.response = res
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
			or error(info, 3)
	end
end

return M
