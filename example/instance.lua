local fiber = require 'fiber'
local log = require 'log'
local config = require 'config'
local yaml = require 'yaml'

local upstream = config.get('app.upstream', {
	etcd = {
		prefix = '/mcs/tarantool/breeze';
	}
})

local proxy = require 'apex.proxy' {
	upstream = upstream;
	timeout = config.get('app.timeout');
	waiting_timeout = config.get('app.waiting_timeout');
	-- on_state_change = f() end
}

proxy:connect()

_G.proxy = proxy
local myapi = {}

_G.api = proxy:autoload(myapi,{
	--[[
		Global params:
			prefix  [= '']
			mode    [= 'rw']
			timeout [= proxy.timeout || 1.1]
	]]
	-- prefix = 'api.';
	-- mode = 'arw';
	mode = 'arw';
	error_status = '500';
	success_status = '200';
	timeout = 10;

	
	on_call = function(ctx,...)
		-- in: local call name + args
		print("Inside on_call ", ctx.func)
		return ctx.func,{'rw','ro'},...
	end;
	request_id = function(func,args)
		-- how to get x-req-id from args;
		return args and args['x-req-id']
	end;
	on_error = function(ctx) -- todo: func
		-- what tuple to return in case of internal error
		-- by default it will raise
		print(yaml.encode(ctx))
		-- ctx.retry = false
		return box.tuple.new{
			500,
			{error = ctx.error; debug = ctx.debug},
			{['x-req-id']=ctx.request_id},
		}
	end;
	on_response = function(ctx,t)
		-- in: local call name + args
		print("Inside on_response")
		-- ctx.retry = false
		
		ctx.status = t[1];
		ctx.debug  = t[2].debug or t[2].error;
		return t
	end;

	-- log = function(t) -- override logging
	-- 	log.info("[%s]\t[END=%s]\tT=%0.3fs\t%s\t%s", t.request_id, t.status, t.run, t.func, t.debug)
	-- 	print(yaml.encode(t))
	-- end;
})

return {}
