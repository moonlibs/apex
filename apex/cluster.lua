local obj = require 'obj'
local log = require 'log'
local sync = require 'sync'
local cstr = require 'string.h'
local fiber = require 'fiber'

local M = obj.class({},'apex.cluster')
local mt = debug.getmetatable(M)

local yaml = require 'yaml'
yaml.cfg{ encode_use_tostring = true }
-- print("class = ",yaml.encode( M ))
-- print("class meta = ",yaml.encode( getmetatable(M) ))

function mt:__serialize()
	return tostring(self)
	-- print("call to __serialize")
	-- return {"xxx"}
end

function M:__serialize()
	-- print("call to __serialize")
	local peers = {}
	for _,v in pairs(self.peers) do
		peers[_] = tostring(v)
	end
	return string.format(
		"[%s] %s (%s)", self.state, self.name, table.concat(peers or {},",")
	)
end

function M:_init( t )
	self.name = 'default'
	for k,v in pairs(t) do
		self[k] = v
	end
	self.state    = 'offine';
	self.peers    = {};
	
	self.primary  = nil;
	self.readonly  = {};
	self.active   = {};
	-- self.rw       = {};
	
	self.waits    = {};
	self.bus      = fiber.channel(1);
end

function M:nodes_info()
	local active = {}
	local readwrite = {}
	local readonly = {}
	for k,v in pairs(self.peers) do
		if cstr.strncmp(v.state,'active',6)==0 then
			table.insert(active,v)
			if v.info.rw then
				table.insert(readwrite,v)
			else
				table.insert(readonly,v)
			end
		end
	end
	return active, readwrite, readonly
end

function M:add(node)
	table.insert(self.peers,node)
	self.peer_count = #self.peers
	node:on('state', function(...)
		print("Cluster registered event",...)
		self.bus:put(true,0)
	end)
end


function M:wait_state()
	return self.bus:get(1)
end

function M:wait_working(timeout)
	timeout = timeout or 1
	local ch = fiber.channel(1)
	self.waits[ch] = ch
	local status = ch:get(1)
	self.waits[ch] = nil
	return self.ok and primary
end

function M:working_state()
	log.info("Entered working state with %s primary", self.primary.addr)
	self.state = 'working'
	self.ok = true
	self.since = fiber.time()
	for ch in pairs(self.waits) do
		ch:put(true)
	end
end

function M:waiting_state(reason)
	log.info("Entered waiting state: %s", reason)
	self.state = 'waiting'
	self.ok = false
	self.since = fiber.time()
	self.primary = nil
	fiber.create(function()
		fiber.sleep(1)
		if self.state == 'waiting' then
			self:failing_state()
		end
	end)
end

function M:failing_state(reason)
	log.info("Entered failing state: %s", reason)
	self.state = 'failing'
	self.ok = false
	self.since = fiber.time()
	self.primary = nil
	for ch in pairs(self.waits) do
		ch:put(false)
	end
end;

-- 		eval = function (cluster,expr,...)
-- 			if type(expr) ~= 'string' then error("call me as method") end
-- 			if self.primary and self.ok then
-- 				local r,info = pcall(function(...) return self.primary.conn:timeout(MATTER_TIMEOUT):eval(...) end, expr, ...)
-- 				if not r then
-- 					return { NULL, info }
-- 				else
-- 					if type(info) == "table" then
-- 						setmetatable(info,nil)
-- 					end
-- 					return info
-- 					-- for _,t in pairs(info) do
-- 					-- 	setmetatable(t,nil)
-- 					-- end
-- 				end
-- 			else
-- 				return { NULL, string.format("Cluster %s is not online", self.name) }
-- 			end
-- 		end;
-- 	},

function M:connect(startup)
	self.fiber = fiber.create(function(reload)
		startup:start()
		local mygen = reload.count
		fiber.name(string.sub(string.format('%s#%s:mon',mygen,self.name),1,32))
		local discovery_cv = sync.cv()
		log.info("Discovery wait...")
		for _,node in pairs(self.peers) do
			node:connect(discovery_cv)
		end
		discovery_cv:wait()
		log.info("Got all nodes info")
		do
			local active, readwrite, readonly = self:nodes_info()
			self.readonly = readonly
			log.info("Got %s of %s active, %s of %s rw", #active, self.peer_count, #readwrite, self.peer_count)
			--[[
				1. discovery
				connect to all nodes and define a quorum
				- all connected
					- one rw: choose it as master
					- none rw: don't work
					- many rw: chose least id
				- not all connected
					- one rw: choose it as master
					- none rw: don't work
					- many rw: don't work
			]]
			if #readwrite == 1 then
				self.primary = readwrite[1]
				self:working_state()
			elseif #readwrite == 0 then
				self:failing_state("no rw nodes")
			else
				if #active == self.peer_count then
					table.sort(readwrite,function(a,b) return a.info.id<b.info.id end)
					self.primary = readwrite[1]
					log.info("Enter working state with %s of %s nodes", #active, self.peer_count)
					self:working_state()
				else
					self:failing_state(string.format("%s rw, %s of %s connected",#readwrite,#active,self.peer_count))
				end
			end
		end

		startup:finish()
		startup = nil

		--[[
			2. change state

			- primary changed rw -> ro | primary gone
				- rw == 0
					: wait 1 for new rw, delay all queries (state=delayed)
				- rw > 0
					: elect new primary

			- ro changed ro -> rw
				- waiting for primary (0 rw)
					: make it primary
				- failstate
					: make it primary, became active
				- have primary
					: ignore
		]]
		while reload.count == mygen do
			if self:wait_state(1) then
				local active, readwrite, readonly = self:nodes_info()
				self.readonly = readonly
				log.info("After change state got %s of %s active, %s of %s rw", #active, self.peer_count, #readwrite, self.peer_count)
				
				if self.primary then
					if not self.primary.info.rw then
						if #readwrite > 0 then
							table.sort(readwrite,function(a,b) return a.info.id<b.info.id end)
							self.primary = readwrite[1]
							log.info("Enter working state with %s of %s nodes", #active, self.peer_count)
							self:working_state()
						else
							if self.state ~= 'waiting' then
								self:waiting_state()
							else
								self:failing_state()
							end
						end
					else
						log.info("State change doesn't affect current primary %s", self.primary.addr);
					end
				else
					if #readwrite > 0 then
						table.sort(readwrite,function(a,b) return a.info.id<b.info.id end)
						self.primary = readwrite[1]
						log.info("Enter working state with %s of %s nodes", #active, self.peer_count)
						self:working_state()
					else
						log.info("State change doesn't affect current state %s", self.state);
					end
				end
			end
		end
	end, package.reload)
end

function M:rw(timeout)
	if self.primary and self.ok then
		-- pass
	elseif self.state == 'waiting' then
		if self:wait_working(timeout) then
			-- pass
		else
			return
		end
	else
		return
	end
	return self.primary
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