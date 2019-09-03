local M = {}

local function compare_deeply(t1, t2, is_recursive)
	if t1 == t2 then
		return true
	end

	for k, v1 in pairs(t1) do
		local v2 = t2[k]
		if type(v1) ~= type(v2) then
			return false
		end

		if type(v1) == 'table' then
			if not compare_deeply(v1, v2) then
				return false
			end
		else
			if v1 ~= v2 then
				return false
			end
		end
	end

	if not is_recursive then
		return compare_deeply(t2, t1, true)
	end

	return true
end

M.compare_deeply = compare_deeply

return M
