local obj = require 'obj'
local log = require 'log'
local fiber = require 'fiber'
local json = require 'json'
local sync = require 'sync'
local reload = package.reload
local netbox = require 'net.box'
local val = require 'val'
local ctx_t = require 'ctx'
-- local caller = require 'devel.caller'
local function caller() return '' end

local M = obj.class({},'apex.node')
local mt = debug.getmetatable(M)

local yaml = require 'yaml'
yaml.cfg{ encode_use_tostring = true }

function mt:__serialize()
	return tostring(self)
end

function M:_stringify()
	return string.format( "[%s] %s (%s/%s)", self.state, self.name, self.addr, self.host )
end

function M:__serialize()
	return tostring(self)
end

local STATE = {}
for _, v in pairs({'active_ro', 'active_rw', 'inactive', 'unavail'}) do
	STATE[string.upper(v)] = v
end

local NODE_PARAMS = {
	addr = {
		type = "state";
	};
	cluster = {
		type = "state";
	};
	name = {
		type = "state";
	};
	proxy_disabled = {
		type    = "state";
		default = false;
	};
	connect_timeout = {
		type    = "dynamic";
		default = 1;
	};
	timeout = {
		type    = "dynamic";
		default = 1;
	};
	reconnect_interval = {
		type    = "dynamic";
		default = 0.3;
	};
	ping_interval = {
		type    = "dynamic";
		default = 0.1;
	};
	ping_timeout = {
		type    = "dynamic";
		default = 0.5;
	};
	fail_max_count = {
		type    = "dynamic";
		default = 10;
	};
	fail_count_printed = {
		type    = "dynamic";
		default = 1;
	};
	disconnect_interval = {
		type    = "dynamic";
		default = 3;
	};
}

local CHANGE_STATE_SEQ = {
	DISCONNECT_TYPE = 'disconnect';
};

function M:get_configuration_params(args)
	local addr = args.instance.box.remote_addr or args.instance.box.listen
	if not addr then error("No address info in " .. args.name, 2) end
	local params = {
		name    = args.name;
		cluster = args.instance.cluster;
		addr    = addr;
		proxy_disabled = args.instance.proxy_disabled or false;
	}
	if args.nodes_conf then
		for k, v in pairs(args.nodes_conf) do
			if not params[k] then
				params[k] = v
			end
		end
	end
	for k, v in pairs(params) do
		if not NODE_PARAMS[k] then
			error(string.format("Extra argument to configure node: %s (%s)", k, v), 2)
		end
	end

	for k, v in pairs(NODE_PARAMS) do
		if not params[k] then
			params[k] = v.default
		end
	end
	return params
end

local init_val = val.idator({
	name = '+string';
	instance = val.req('table', {
		box = val.req('table', {
			remote_addr = '?string';
			listen      = '?string';
		});
		cluster = '+string';
		proxy_disabled = '?boolean';
	});
	nodes_conf = val.opt('table', {
		connect_timeout     = val.opt(val.num);
		timeout             = val.opt(val.num);
		reconnect_interval  = val.opt(val.num);
		ping_interval       = val.opt(val.num);
		ping_timeout        = val.opt(val.num);
		fail_max_count      = val.opt(val.num);
		disconnect_interval = val.opt(val.num);
	});
})
function M:_init(args)
	init_val(args)
	local params = self:get_configuration_params(args)
	for k,v in pairs(params) do self[k] = v end

	self.gen   = package.reload.count
	self.info  = {}
	self.host  = 'unknown'
	self.state = STATE.INACTIVE
	self._seen = {}
	self._evs  = {}
	self.ctx   = ctx_t( string.format("node:%s", self.name) )
	self.ctx.log.store = nil
end

function M:update(args, cv)
	init_val(args)
	local params = M:get_configuration_params(args)

	local require_state_update = false
	for k, v in pairs(NODE_PARAMS) do
		if v.type == 'state' and self[k] ~= params[k] then
			self.ctx.log:warn("Try to change %s: %s -> %s. Static parameters require update with change state.",
				k, self[k], params[k])
			require_state_update = true
		end
	end

	if require_state_update then
		if params.proxy_disabled ~= self.proxy_disabled then
			if params.proxy_disabled == true then
				for k, _ in pairs(NODE_PARAMS) do self[k] = params[k] end
				self:disconnect(cv)
				self.ctx.log:info("updated with disconnect")
			else
				for k, _ in pairs(NODE_PARAMS) do self[k] = params[k] end
				self:connect(cv)
				self.ctx.log:info("updated with connect")
			end
		else
			for k, _ in pairs(NODE_PARAMS) do self[k] = params[k] end
			self:reconnect(cv)
			self.ctx.log:info("updated with reconnect")
		end
	else
		for k, v in pairs(NODE_PARAMS) do
			if v.type == 'dynamic' and self[k] ~= params[k] then
				self.ctx.log:info("update. Change %s: %s -> %s", k, self[k], params[k])
				self[k] = params[k]
			end
		end
		self.ctx.log:info("updated dynamically")
	end
end

function M:get_params(param_type)
	local res = {}
	for k, v in pairs(NODE_PARAMS) do
		if not param_type or param_type == v.type then
			res[k] = self[k]
		end
	end
	return res
end

function M:on(event, cb)
	if not self._evs[event] then
		self._evs[event] = {}
	end
	table.insert(self._evs[event], cb)
end

function M:no(event, cb)
	if not self._evs[event] then
		return
	end
	for k,v in pairs(self._evs[event]) do
		if v == cb then
			table.remove(self._evs[event], k)
			return
		end
	end
end

function M:event(event, ...)
	if self._evs[event] then
		for _, cb in pairs(self._evs[event]) do
			cb(self, event, ...)
		end
	end
end

function M:set_host(host)
	local old = self.host
	self.host = host
	if old ~= host then
		self:event('host', host, old)
	end
end

function M:set_state(state, args)
	if not args then args = {} end

	local now = fiber.time()
	if not STATE[string.upper(state)] then
		error(string.format("unknown state %s for node %s", state, self.name))
	end

	local csts = args.change_state_seq
	if self.change_state_seq then
		if not csts then
			-- self.ctx.log:info("Node in sequence %s. Impossible to change state %s -> %s",
			-- 	self.change_state_seq.type, self.state, state)
			return false
		end
		if csts.type ~= self.change_state_seq.type then
			self.ctx.log:info("current sequence %s. Impossible to change state %s -> %s in another sequence %s",
				self.change_state_seq.type, self.state, state, csts.type)
			return false
		end
		if csts.finish then
			self.ctx.log:info("finish sequence %s", self.change_state_seq.type)
			self.change_state_seq = nil
		end
	elseif csts then
		self.change_state_seq = csts
		self.ctx.log:info("start sequence %s", self.change_state_seq.type)
	end

	local old = self.state
	self.state = state
	if old ~= state then
		self.state_mtime = now
		if state == STATE.INACTIVE then
			self.info.rw = nil
		end
		self:event('state', state, old)
		if state == STATE.UNAVAIL or state == STATE.INACTIVE then
			self.ctx.log:info("%s -> %s at %s", old, state, caller(1))
		else
			self.ctx.log:info("%s -> %s at %s", old, state, caller(1))
		end
	end
	return true
end

function M:describe()
	return string.format("%s/%s:%s", self.addr, self.info and self.info.host, self.state)
end

function M:disconnect(cv)
	if self.change_state_seq and self.change_state_seq.type == CHANGE_STATE_SEQ.DISCONNECT_TYPE then
		self.ctx.log:info("node already try to disconnect. Ignore with function")
		return
	end

	if cv then cv:start() end
	self:set_state( STATE.UNAVAIL, { change_state_seq = {
		type = CHANGE_STATE_SEQ.DISCONNECT_TYPE;
		start = fiber.time();
	}} )

	local disconnect_time = fiber.time() + self.disconnect_interval

	if self.state_mtime > disconnect_time then
		self:set_state( STATE.INACTIVE, { change_state_seq = {
			type = CHANGE_STATE_SEQ.DISCONNECT_TYPE;
			finish = fiber.time();
		}} )
		self.conn:close()
		if cv then
			local ok, r = pcall(function() cv:finish() end)
			if not ok then
				self.ctx.log:error("Error in finish cv in node disconnect. Count=%d", cv.count)
			end
			cv = nil
		end
	else
		fiber.create(function()
			fiber.sleep(disconnect_time - self.state_mtime)
			if package.reload.count == self.gen then
				self:set_state( STATE.INACTIVE, { change_state_seq = {
					type = CHANGE_STATE_SEQ.DISCONNECT_TYPE;
					finish = fiber.time();
				}} )
				self.conn:close()
			end
			if cv then
				local ok, r = pcall(function() cv:finish() end)
				if not ok then
					self.ctx.log:error("Error in finish cv in node disconnect (in waiting fiber). Count=%d", cv.count)
				end
				cv = nil
			end
		end)
	end
end

function M:reconnect(cv)
	if cv then cv:start() end

	local reconnect_cv = sync.cv()
	self:disconnect(reconnect_cv)
	self:connect(reconnect_cv)
	reconnect_cv:wait()

	if cv then
		local ok, r = pcall(function() cv:finish() end)
		if not ok then
			self.ctx.log:error("Error in finish cv in node reconnect. Count=%d", cv.count)
		end
		cv = nil
	end
end

function M:connect(cv)
	if self.proxy_disabled == true then
		self.ctx.log:info("node was disabled")
		return
	end
	self.fiber = fiber.create(function()
		cv:start()
		fiber.sleep(0.01)
		fiber.name( string.sub(string.format('%s:cluster_node', self.gen), 1, 32) )

		while package.reload.count == self.gen do
			local r, e = pcall(function()
				self.conn = netbox.new(self.addr, {
					wait_connected  = false,
					connect_timeout = self.connect_timeout;
					timeout         = self.timeout;
					call_16         = true,
				})

				self.info = {}
				self.conn:wait_connected(1)

				if self.conn.state ~= 'active' then
					if cv then
						local ok, r = pcall(function() cv:finish() end)
						if not ok then
							self.ctx.log:error("Error in finish cv in node connect (in case with not active conn state). Count=%d", cv.count)
						end
						cv = nil
					end
					fiber.sleep(self.reconnect_interval)
				else
					local failcount = 0
					while self.conn.state == 'active' and self.gen == package.reload.count do
						local state = self.state
						local r, info = pcall(function() return self.conn:timeout(self.ping_timeout):call('kit.node', {}) end)
						if r and info then
							failcount = 0
							self.info = info[1][1]
							self.info.host = ( self.info.hostname:match('^([^.]+)') )
							self:set_host(self.info.host)
							if self.info.rw then
								self:set_state( STATE.ACTIVE_RW )
							else
								self:set_state( STATE.ACTIVE_RO )
							end
						else
							if not self.change_state_seq or self.change_state_seq.type ~= CHANGE_STATE_SEQ.DISCONNECT_TYPE then
								if failcount < self.fail_count_printed then
									self.ctx.log:info("kit.node call failed: %s", info)
								end
								self:set_state( STATE.UNAVAIL )
								failcount = failcount + 1
								if failcount > self.fail_max_count then
									self.ctx.log:info("node closing because of fails = %s", failcount)
									self:disconnect()
								end
							end
						end

						if cv then
							local ok, r = pcall(function() cv:finish() end)
							if not ok then
								self.ctx.log:error("Error in finish cv in node connect (in case with ping connection). Count=%d", cv.count)
							end
							cv = nil
						end
						fiber.sleep(self.ping_interval)
					end

					self:set_state( STATE.INACTIVE )
					self.ctx.log:info("node disconnected. Conn: (%s) %s. PackageGen (%s/%s)",
						self.conn.state, self.conn.error, self.gen, package.reload.count)
				end
			end)
			if not r then
				self.ctx.log:info("fiber failed: %s", e)
				fiber.sleep(0.01)
			end
			if self.proxy_disabled == true then
				self.ctx.log:info("node was disabled")
				break
			end
		end
		self.ctx.log:info("obsoleted")
	end)
end

return M
