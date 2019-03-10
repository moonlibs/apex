local obj = require 'obj'
local log = require 'log'
local fiber = require 'fiber'
local reload = package.reload
local netbox = require 'net.box'
-- local caller = require 'devel.caller'
local function caller() return '' end

--[[
states:
	error E
	active A
		active-ro AR
		active-rw AW
	offline F
	unavail X

]]

local M = obj.class({},'apex.node')
local mt = debug.getmetatable(M)

local yaml = require 'yaml'
yaml.cfg{ encode_use_tostring = true }

local SILENT = false

function mt:__serialize()
	return tostring(self)
end

function M:_stringify()
	return string.format( "[%s] %s (%s/%s)", self.state, self.name, self.addr, self.host )
end

function M:__serialize()
	return tostring(self)
end

function M:_init( t )
	self.reconnect_interval = 0.3;
	self.ping_interval = 0.1;
	self.fail_count = 10;
	self.ping_timeout = 0.5;
	for k,v in pairs(t) do
		self[k] = v
	end
	self.host  = 'unknown';
	self.state = 'offine';
	self._seen = {};
	self._evs  = {};
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
function M:set_state(state)
	local old = self.state
	self.state = state
	if old ~= state then
		self:event('state',state,old)
		if state:match('error') then
			log.info("[ALERT] %s/%s: %s -> %s  at %s",
				self.addr, self.info.host, old, state, caller(1))
		else
			log.info("%s/%s: %s -> %s  at %s",
				self.addr, self.info.host, old, state, caller(1))
		end
	end
end

function M:describe()
	return string.format("%s/%s:%s", self.addr, self.info and self.info.host, self.state)
end

function M:connect(cv)
	self.fiber = fiber.create(function()
		-- cv:start()
		-- fiber.sleep(0.01)
		local mygen = reload.count
		fiber.name(string.sub(string.format('%s#%s:%s',mygen,self.name,self.addr),1,32))
		
		local warn = log.warn
		local warned = false
		while reload.count == mygen do
			local r,e = pcall(function()
				self.conn = netbox.new(self.addr, {
					wait_connected = false,
					connect_timeout = 1, -- TODO
					timeout = 1,
					call_16 = true,
				})
				if getmetatable( self.conn ) and not rawget( getmetatable( self.conn ), "__serialize" ) then
					rawset( getmetatable( self.conn ), "__serialize", function(c)
						return string.format("%s:%s [%s] (%s)",c.host,c.port,c.state,c.error)
					end )
				end
				self.info = { rw = nil }

				if SILENT then
					log.warn = function() end
				end

				repeat
					local state = self.conn:wait_connected(1)
					state = self.conn.state
					if state == 'active' then break end
					if state == 'handshake' then
						state = 'error'
						self.conn.error = 'Connection timed out'
					end
					if self.state ~= state then
						log.error("Failed to connect to %s: (%s) %s", self.addr, self.conn.state, self.conn.error)
					end
					self:set_state(state)
					if cv then cv:finish() cv = nil end
					fiber.sleep(self.reconnect_interval)
				until true

				if SILENT then
					log.warn = warn
				end

				if self.conn.state ~= 'active' then
					if not warned then
						log.info("Node %s: %s", self.addr, self.conn.error)
						warned = true
					end
				else -- if self.conn.state == 'active' then
					warned = false
					local info
					local failcount = 0
					while self.conn.state == 'active' and mygen == reload.count do
						local r,info = pcall(function() return self.conn:timeout(self.ping_timeout):call('kit.node') end)
						if r and info then
							failcount = 0
							local vinfo = info[1][1]
							if not self.info.id then
								log.info("Node %s/%s connected with rw %s", self.addr, vinfo.hostname, vinfo.rw)
							end
							self.info = vinfo
							self.info.host = (self.info.hostname:match('^([^.]+)'))
							self:set_host(self.info.host)
							self:set_state('active-' .. ( vinfo.rw and "rw" or "ro" ))
						else
							log.info("Node %s kit.node call failed: %s", self.addr, info)
							self:set_state('unavail')
							failcount = failcount + 1
							if failcount > self.fail_count then
								log.info("Node %s closing because of fails", self.addr)
								self.conn:close()
							end
						end
						
						if cv then cv:finish() cv = nil end
						fiber.sleep(self.ping_interval)
					end

					self.info.rw = nil
					self.error = self.conn.error
					self:set_state(self.conn.state)

					log.info("Node %s/%s disconnected: (%s) %s", self.addr, self.host, self.conn.state, self.conn.error)
				end
			end)
			if not r then
				log.error("Fiber failed: %s", e)
				fiber.sleep(0.01)
			end
		end
		log.info("Obsoleted")
	end)
end

return M