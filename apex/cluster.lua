local obj = require 'obj'
local log = require 'log'
local sync = require 'sync'
local cstr = require 'string.h'
local fiber = require 'fiber'
local val = require 'val'
local ctx_t = require 'ctx'

local M = obj.class({},'apex.cluster')
local mt = debug.getmetatable(M)

local yaml = require 'yaml'
yaml.cfg{ encode_use_tostring = true }
-- print("class = ",yaml.encode( M ))
-- print("class meta = ",yaml.encode( getmetatable(M) ))

local STATE = {}
for _, v in pairs({'working', 'waiting', 'failing', 'offline' }) do
	STATE[string.upper(v)] = v
end

function mt:__serialize()
	return tostring(self)
end

function M:__serialize()
	local nodes = {}
	for name, v in pairs(self.nodes) do
		nodes[ name ] = tostring(v)
	end
	return string.format(
		"[%s] %s (%s)", self.state, self.name, table.concat(nodes or {},",")
	)
end

function M:_get_nodes_params(args)
	local nodes_args = {}
	local instance_set = config.etcd:list( self.upstream.etcd.prefix.."/instances" )
	for name, conf in pairs(instance_set) do
		if conf.cluster == self.name then
			table.insert(nodes_args, {
				name       = name;
				instance   = conf;
				nodes_conf = args.nodes_conf
			})
		end
	end
	if #nodes_args == 0 then error("Peeked no nodes by current config for cluster " .. self.name, 0) end
	return nodes_args
end

local init_val = val.idator({
	name     = "+string";
	upstream = val.req("table", {
		etcd = val.req("table", {
			prefix = "+string";
		});
	});
	nodes_conf = val.opt("table", {
		connect_timeout    = val.opt(val.num);
		timeout            = val.opt(val.num);
		reconnect_interval = val.opt(val.num);
		ping_interval      = val.opt(val.num);
		ping_timeout       = val.opt(val.num);
		fail_max_count     = val.opt(val.num);
	});
	failing_timeout = val.opt(val.num);
})
function M:_init(t)
	init_val(t)

	self.name = t.name
	self.upstream = t.upstream
	self.failing_timeout = t.failing_timeout or 3

	self.state       = STATE.OFFLINE
	self.nodes       = {}
	self.nodes_count = 0
	self.readwrite   = nil
	self.readonly    = {}
	self.waits       = {}
	self.bus         = fiber.channel(1)
	self.gen         = package.reload.count
	self.ctx         = ctx_t("cluster:"..t.name)
	self.ctx.log.store = nil

	local nodes_args = self:_get_nodes_params({ nodes_conf = t.nodes_conf })
	for _, node_args in pairs( nodes_args ) do
		local node = require 'apex.node'(node_args)
		self:add_node(node)
	end
end

function M:update(args, discovery_cv)
	init_val(args)
	self.ctx.log:info("start update cluster")
	if self.name ~= args.name then
		error("Impossible to update name", 2)
	end
	if self.upstream.etcd.prefix ~= args.upstream.etcd.prefix then
		error("Impossible to update upstream.etcd.prefix", 2)
	end
	local nodes_args = self:_get_nodes_params({ nodes_conf = args.nodes_conf })
	for _, node_args in pairs( nodes_args ) do
		if not self.nodes[ node_args.name ] then
			local node = require 'apex.node'(node_args)
			self:add_node(node)
			node:connect(discovery_cv)
		else
			self.nodes[ node_args.name ]:update(node_args, discovery_cv)
		end
	end
	self.ctx.log:info("finish update cluster")
end

function M:nodes_info()
	local res = {
		cnt = {
			proxy_enabled  = 0;
			proxy_disabled = 0;
			active    = 0;
			inactive  = 0;
			readwrite = 0;
			readonly  = 0;
		};
		list = {
			readonly  = {};
			readwrite = {};
		};
	}
	for k, v in pairs(self.nodes) do
		if v.proxy_disabled then
			res.cnt.proxy_disabled = res.cnt.proxy_disabled + 1
		else
			if cstr.strncmp(v.state, 'active', 6) == 0 then
				if v.info.rw then
					res.cnt.readwrite = res.cnt.readwrite + 1
					table.insert(res.list.readwrite, v)
				else
					res.cnt.readonly = res.cnt.readonly + 1
					table.insert(res.list.readonly, v)
				end
			else
				res.cnt.inactive = res.cnt.inactive + 1
			end
		end
	end
	res.cnt.active        = res.cnt.readwrite + res.cnt.readonly
	res.cnt.proxy_enabled = res.cnt.active + res.cnt.inactive
	return res
end

function M:nodes_state()
	local res = {}
	for name, struct in pairs(self.nodes) do
		res[name] = {
			state = struct.state;
		}
	end
	return res
end

function M:remove_node(node)
	self.nodes[node.name]:disconnect()
	self.nodes_count = self.nodes_count - 1
	self.nodes[node.name] = nil
end

function M:add_node(node)
	self.nodes[node.name] = node
	self.nodes_count = self.nodes_count + 1
	node:on('state', function(...)
		-- print("Require check cluster. Event was registered: ", ...)
		self.bus:put(true, 0)
	end)
end

function M:wait_state()
	return self.bus:get(1)
end

function M:wait_working(timeout)
	timeout = timeout or 1
	local ch = fiber.channel(1)
	self.waits[ch] = ch
	local status = ch:get(timeout)
	self.waits[ch] = nil
	return self.ok
end

function M:working_state(nodes)
	if nodes then
		self.ctx.log:info("entered working state with %d/%d enabled nodes (from %s). Primary: %s",
			nodes.cnt.active, nodes.cnt.proxy_enabled, self.nodes_count, self.readwrite.addr)
	else
		self.ctx.log:info("entered working state. Primary: %s", self.readwrite.addr)
	end
	self.state = STATE.WORKING
	self.ok = true
	self.since = fiber.time()
	self.failing_reason = nil
	self.waiting_reason = nil
	for ch in pairs(self.waits) do
		ch:put(true)
	end
end

function M:waiting_state(reason)
	self.ctx.log:info("entered waiting state: %s", reason)
	self.state = STATE.WAITING
	self.ok    = false
	self.since = fiber.time()
	self.waiting_reason = reason
	self.readwrite = nil
	if self.gen == package.reload.count then
		if self.failing_fiber and self.failing_fiber:status() ~= "dead" then
			self.ctx.log:info("not start failing fiber - its already exist. Id=%s. Status=%s",
				self.failing_fiber:id(), self.failing_fiber:status()
			)
			return
		end
		self.failing_fiber = fiber.create(function()
			fiber.name( string.sub(string.format('failing_fb:%s:%s:%s', self.name, package.reload.count, fiber.self().id()), 1, 32) )
			repeat
				if package.reload.count ~= self.gen then
					self.ctx.log:info("finish failing_fiber - change gen")
					break
				end
				if self.state ~= STATE.WAITING then
					self.ctx.log:info("not call failing_state in failing_fiber. Cluster is %s", self.state)
					break
				end

				local fail_at = math.floor(self.since) + self.failing_timeout
				local now = math.floor(fiber.time())

				if fail_at <= now then
					self:failing_state(self.waiting_reason)
					break
				end
				local diff = fail_at - now
				self.ctx.log:info("call failing_state at %s after %s sec",
					fail_at, diff
				)
				fiber.sleep(diff)
			until false
			self.ctx.log:info("finished failing fiber id=%s", self.failing_fiber:id())
			self.failing_fiber = nil
		end)
		self.ctx.log:info("started failing fiber id=%s", self.failing_fiber:id())
	else
		self.ctx.log:info("not start failing fiber - change gen")
	end
end

function M:failing_state(reason)
	self.ctx.log:info("entered failing state: %s", reason)
	self.state = STATE.FAILING
	self.ok = false
	self.since = fiber.time()
	self.failing_reason = reason
	self.readwrite = nil
	for ch in pairs(self.waits) do
		ch:put(false)
	end
end

function M:connect(startup_cv)
	self.fiber = fiber.create(function(reload)
		startup_cv:start()

		fiber.name( string.sub(string.format('%s:cluster', self.gen), 1, 32) )

		local discovery_cv = sync.cv()
		self.ctx.log:info("start connect cluster_nodes")
		for name, node in pairs(self.nodes) do
			node:connect(discovery_cv)
		end
		discovery_cv:wait()
		self.ctx.log:info("finish connect cluster_nodes")

		self.bus:put(true, 0) -- if all the nodes do not change the status

		while reload.count == self.gen do
			if self:wait_state() then
				local nodes = self:nodes_info()

				self.ctx.log:info("change state. Got %s/%s enabled nodes (from %s), %s readonly, %s readwrite",
					nodes.cnt.active, nodes.cnt.proxy_enabled,
					self.nodes_count, nodes.cnt.readonly, nodes.cnt.readwrite
				)

				self.readonly  = nodes.list.readonly
				if self.readwrite and self.readwrite.info.rw then
					self.ctx.log:info("state change doesn't affect current primary %s", self.readwrite.addr)
				else
					if #nodes.list.readwrite == 1 then
						self.readwrite = nodes.list.readwrite[1]
						self:working_state(nodes)
					else
						self:waiting_state(string.format("Count of readwrite nodes=%s", #nodes.list.readwrite))
					end
				end

				if startup_cv then
					local ok, r = pcall(function() startup_cv:finish() end)
					if not ok then
						self.ctx.log:error("Error in finish startup_cv in cluster connect. Count=%d", startup_cv.count)
					end
					startup_cv = nil
				end
			end
		end
	end, package.reload)
end

function M:rw(timeout)
	if self.readwrite and self.ok then
		-- pass
	elseif self.state == STATE.WAITING then
		if self:wait_working(timeout) then
			-- pass
		else
			return
		end
	else
		return
	end
	return self.readwrite
end

function M:ro(timeout)
	return self.readonly[ math.random(#self.readonly) ]
end

function M:arw(timeout)
	return self:rw(timeout) or self.readonly[ math.random(#self.readonly) ]
end

function M:aro(timeout)
	return self.readonly[ math.random(#self.readonly) ] or self:rw(timeout)
end

return M
