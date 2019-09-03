local obj = require 'obj'
local fiber = require 'fiber'
local sync = require 'sync'
local val = require 'val'
local netbox = require 'net.box'
local NULL = require 'msgpack'.NULL
local ctx_t = require 'ctx'

-- local caller = require 'devel.caller'
local function caller() return '' end

local M = obj.class({},'apex.apex_instances')
local mt = debug.getmetatable(M)

local APEXES_PARAMS = {
	connect_timeout = {
		default = 1;
	};
	timeout = {
		default = 1;
	};
	reconnect_interval = {
		default = 0.3;
	};
	ping_interval = {
		default = 0.1;
	};
	ping_timeout = {
		default = 0.5;
	};
	fail_max_count = {
		default = 10;
	};
	disconnect_interval = {
		default = 3;
	};	
}

local init_val = val.idator({
	etcd = val.req("table", {
		prefix = "+string";
	});
})
function M:_init(args)
	init_val(args)
	self.ctx = ctx_t("apex_intances")
	self.ctx.log.store = nil
	self.etcd = args.etcd

	local etcd_config = config.etcd:list(args.etcd.prefix)
	local instances = {}
	for k, v in pairs(etcd_config.instances) do
		instances[k] = v.box.listen
	end
	local current_instance_name = config.get('sys.instance_name')
	instances[current_instance_name] = nil

	if etcd_config.common and etcd_config.common.app and etcd_config.common.app.apexes then
		local apexes_conf = etcd_config.common.app.apexes
		for k, v in pairs(apexes_conf) do
			if not APEXES_PARAMS[k] then
				error(string.format("Extra argument to configure apex_instances: %s (%s)", k, v))
			end
			self[k] = v
		end
		for k, v in pairs(APEXES_PARAMS) do
			if not self[k] then
				self[k] = v.default
			end
		end
	end

	self.instances = {}
	for name, addr in pairs(instances) do
		local apex = {}
		apex.fiber = fiber.create(function()
			fiber.yield(0)
			local mygen = package.reload.count
			fiber.name(string.format("%s:%s", mygen, name))
			while mygen == package.reload.count do
				local r, e = pcall(function()
					apex.conn = netbox.new(addr, {
						wait_connected  = false;
						connect_timeout = self.connect_timeout;
						timeout         = self.timeout;
						call_16         = true,
					})
					apex.conn:wait_connected(1)
					if apex.conn.state ~= 'active' then
						fiber.sleep(self.reconnect_interval)
					else
						local failcount = 0
						while apex.conn.state == 'active' do
							if  mygen ~= package.reload.count then
								self.ctx.log:info("apex disconnecting because of changing gen")
								apex.conn:close()
								break
							end
							local r, info = pcall(function()
								return apex.conn:timeout(self.ping_timeout):call('kit.node', {})
							end)
							if r and info then
								failcount = 0
							else
								failcount = failcount + 1
								if failcount > self.fail_max_count then
									self.ctx.log:info("apex disconnecting because of fails")
									apex.conn:close()
								end
							end
							fiber.sleep(self.ping_interval)
						end
						self.ctx.log:info("apex disconnected. Conn: (%s) %s", apex.conn.state, apex.conn.error)
					end
				end)
				if not r then
					self.ctx.log:info("fiber failed: %s", e)
					fiber.sleep(0.01)
				end
			end
			self.ctx.log:info("obsoleted")
		end)
		self.instances[name] = apex
	end
end

function M:call(method, args)
	if not args then args = {} end
	local struct = {}
	for name, apex_instance in pairs(self.instances) do
		local res
		if apex_instance.conn.state ~= 'active' then
			res = { NULL, string.format("Dont connect to apex instance %s", name) }
		else
			local ok, info = pcall(function(...)
				return apex_instance.conn:timeout(self.timeout):call(...)
			end, method, args)
			if not ok then
				res = { NULL, info }
			else
				res = info[1]
			end
		end
		struct[name] = res
	end
	return struct
end

return M
