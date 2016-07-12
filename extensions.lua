--Put on /etc/asterisk
--Need pbx_lua.so

REDIS_SERVER = 'cti-redis'
REDIS_PORT = 6379

MYSQL_SERVER = 'cti-mysql'
MYSQL_PORT = 3306
MYSQL_DB = 'cti'
MYSQL_USER = 'asterisk'
--MYSQL_PWD = 'maxxday123'
MYSQL_PWD = 'dreamstart'

ENU_EXTERNAL = '192.168.11.49'
--ENU_EXTERNAL = '172.17.2.95'

RECORD_MP3 = true

ES_UNAVAILIABLE = '0'
ES_REGISTER = '1'
ES_IDLE = '2'
ES_DIALING = '3'
ES_TALKING_IN = '4'
ES_TALKING_OUT = '5'
ES_HOLDIN = '6'
ES_HOLDOUT = '7'
ES_SPYING = '8'
ES_WHISPER = '9'
ES_RINGING = '10'

NT_UNKNOWN = '0'
NT_EXTERNAL = '1'
NT_EXTENSION = '2'
NT_QUEUE = '3'
NT_GROUP = '4'
NT_MEETME = '5'
NT_IVR = '6'


CDR_CALL_TYPE_inbound = '1'
CDR_CALL_TYPE_pv = '2'
CDR_CALL_TYPE_callback = '3'
CDR_CALL_TYPE_transfer = '4'
CDR_CALL_TYPE_spy = '5'
CDR_CALL_TYPE_whisper = '6'

CDR_TARGET_TYPE_exten = '1'
CDR_TARGET_TYPE_ivr = '2'
CDR_TARGET_TYPE_external = '3'
CDR_TARGET_TYPE_queue = '4'

CDR_CALL_CAUSE_normal = '0'
CDR_CALL_CAUSE_no_route = '1'
CDR_CALL_CAUSE_target_no_answer = '2'
CDR_CALL_CAUSE_failed = '3'
CDR_CALL_CAUSE_unknow = '4'

CDR_SRC_TYPE_none = '0'
CDR_SRC_TYPE_exten = '1'
CDR_SRC_TYPE_ivr = '2'
CDR_SRC_TYPE_external = '3'
CDR_SRC_TYPE_queue = '4'

redis = require 'redis'
redis_client = redis.connect(REDIS_SERVER, REDIS_PORT)
mysql = require 'luasql.mysql'
env = mysql.mysql()
mysql_conn = env:connect(MYSQL_DB,MYSQL_USER,MYSQL_PWD,MYSQL_SERVER,MYSQL_PORT)

function pathchunks(name)
	local chunks = {}
	for w in string.gmatch(name, "[^/\\]+") do
		table.insert(chunks, 1, w)
	end
	return chunks
end

function ensurepath(path)
	--luarocks install luafilesystem
	local lfs = require 'lfs'
	local chunks = pathchunks(path)
	local originalpath = lfs.currentdir()
	lfs.chdir("/")
	for i=#chunks, 1, -1 do
		local c = chunks[i]
		local exists = lfs.attributes(c) ~= nil
		if(not exists) then
			lfs.mkdir(c)
		end
		lfs.chdir(c)
	end
	lfs.chdir(originalpath)
	return path
end


function redis_hset(key,field,value)
	if(value == nil) then
		value = ''
	end

	redis_client:hset(key,field,value)
end


function redis_hget(key,field)
	local value = redis_client:hget(key, field)
	if(value == nil) then
		value = ''
	end
	return value
end


function redis_get(key)
	local value = redis_client:get(key)
	if(value == nil) then
		value = ''
	end
	return value
end

--[[
function string.split(str, delimiter)
	if str==nil or str=='' or delimiter==nil then
		return nil
	end

	local result = {}
	for match in (str..delimiter):gmatch("(.-)"..delimiter) do
		table.insert(result, match)
	end
	return result
end
]]

function string.split(str, delim, maxNb)
-- Eliminate bad cases...   
	if string.find(str, delim) == nil then
		return { str }
	end
	if maxNb == nil or maxNb < 1 then
		maxNb = 0    -- No limit   
	end
	local result = {}
	local pat = "(.-)" .. delim .. "()"
	local nb = 0
	local lastPos = 1
	for part, pos in string.gfind(str, pat) do
		nb = nb + 1
		if nb == maxNb then break end
		result[nb] = part
		lastPos = pos
	end
	-- Handle the last field   
	if nb ~= maxNb then
		result[nb + 1] = string.sub(str, lastPos)
	else
		result[nb] = string.sub(str,lastPos)
	end
	return result
end


function sleep(n)
	--os.execute('sleep ' .. n)
	local socket = require 'socket'
	socket.select(nil, nil, n)
end


function get_curtime()
	local socket = require 'socket'
	return socket.gettime()
end


function make_callid(extension)
	extension = extension or ''
	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()
	--return ast_addr .. '.' .. get_curtime() .. '.' .. extension
	return ast_addr .. '.' .. get_curtime()
end

function set_exten_channel(exten, ch)
	redis_hset('tsr:'..exten, 'channel', ch)
end


function set_exten_ast_addr(exten, ast_addr)
	redis_hset('tsr:'..exten, 'asterisk', ast_addr)
end


function set_exten_peer_channel(exten, ch) 
	return redis_hset('tsr:'..exten, 'peer_channel', ch)
end


function get_exten_status(exten) 
	return redis_hget('tsr:'..exten, 'status')
end


function get_queue_group(queue)
	return redis_hget('queue:'..queue, 'group')
end


function get_exten_group(exten)
	return redis_hget('tsr:'..exten, 'group')
end


function get_accessnum_group(accessnum)
	return redis_hget('accnum:'..accessnum, 'group')
end


function get_exten_channel(exten)
	return redis_hget('tsr:'..exten, 'channel')
end

function set_queue_busy(queue,exten)
	return redis_client:sadd('queue:'..queue..':busy', exten)
end

function del_queue_busy(queue,exten)
	return redis_client:srem('queue:'..queue..':busy', exten)
end

function del_exten_from_queue(queue,exten)
	return redis_client:zrem('queue:'..queue..':tsr', exten)
end

function del_queue_from_exten(exten, queue)
	return redis_client:zrem('tsr:'..exten..':queue', queue)
end

function exten_is_idle(exten)
	local exten_status = get_exten_status(exten)
	app.Verbose('exten:'..exten..',status:'..exten_status)
	if (ES_IDLE == exten_status or ES_REGISTER == exten_status) then
		return true
	else
		return false
	end
end


function get_exten_opensips_addr(exten)
	return redis_hget('tsr:'..exten, 'opensips') 
end


function write_cdr(data)
	local cloumn =  "`callid`,`call_type`,`target_type`,`target`,`target_record`,`target_groupid`,`src_type`,`src`,`caller`,`caller_uniqueid`,`callee`,`callee_uniqueid`,`did`,`starttime`,`ringtime`,`answertime`,`bridgetime`,`endtime`,`call_cause`"
	local values =  "'"..data['callid'].."','"..data['call_type'].."','"..data['target_type'].."','"..data['target'].."','"..data['target_record'].."','"..data['target_groupid'].."','"..data['src_type'].."','"..data['src'].."','"..data['caller'].."','"..data['caller_uniqueid'].."','"..data['callee'].."','"..data['callee_uniqueid'].."','"..data['did'].."','"..data['starttime'].."','"..data['ringtime'].."','"..data['answertime'].."','"..data['bridgetime'].."','"..data['endtime'].."','"..data['call_cause'].."'"

	local sql = "INSERT INTO cdr ("..cloumn..") VALUES ("..values..");"

	app.Verbose(sql)

	local status,errorString = mysql_conn:execute(sql)
	status = status or ''
	errorString = errorString or ''
	app.Verbose('Write CDR ['..status..'.'..errorString..'].')
end


function exten_has_record(exten)
	if('1' == redis_hget('tsr:'..exten, 'record')) then
		return true
	end

	return false
end


function record_exten(cti_callid, exten)
	if(exten_has_record(exten)) then
		local key = 'call-context:'..cti_callid
		local groupid = get_exten_group(exten)
		if (groupid == '') then
			groupid = '000000'
		end
		starttime = redis_hget(key, 'starttime')

		if (starttime ~= '') then
			if(RECORD_MP3) then
				local yyyymmdd = os.date("%Y%m%d",starttime)
				local ymd = os.date("%Y/%m/%d",starttime)
				local record_name = groupid..'_'..yyyymmdd..'_'..cti_callid..'-'..exten
				local record_path = '/data/cti_nas/media/record/'..ymd..'/'
				ensurepath(record_path)
				local wav_file = record_path..record_name..'.wav';
				app.MixMonitor(wav_file..',,/usr/local/bin/lame '..wav_file..' && /bin/rm '..wav_file)
				redis_hset(key, 'record', record_name)
			else
				local yyyymmdd = os.date("%Y%m%d",starttime)
				local record_name = groupid..'_'..yyyymmdd..'_'..cti_callid..'-'..exten
				local record_path = '/REC/'
				local wav_file = record_path..record_name..'.wav';
				app.MixMonitor(wav_file)
				redis_hset(key, 'record', record_name)
			end
		end
	end
end


function get_gw_info(number)
	local sql = 'select b.name,b.area,b.type from displaynum as a join telecom_gateway as b on a.gateway=b.name where a.number='..number
	local cursor,errorString = mysql_conn:execute(sql)

	local result = {}

	if(errorString == nil) then
		local i = 0;
		local row = cursor:fetch ({}, "a")
		while row do
			i = i + 1
			result[i] = row
			row = cursor:fetch (row, "a")
		end
		cursor:close()
	end

	return result
end


function get_mobile_region(number)
	number = number or ''
	local sql = 'select area from region_mobile_edge where mobile_edge='..number
	local cursor,errorString = mysql_conn:execute(sql)

	local result = false

	if(errorString == nil) then
		local row = cursor:fetch ({}, "a")
		if(row ~= nil) then
			result = row.area
		end
		cursor:close()
	end

	return result
end


function check_allow_external_num(external_num)
	array = {'00','168','1259','1258','198','195','268','400'}

	for k,v in pairs(array) do
		if string.sub(external_num,1,string.len(v)) == v then
			return false
		end
	end

	return true
end


function dial_external_num(cti_callid, display_num, external_num)
	--channel['CHANNEL(hangup_handler_push)'] = 'hangup-dial,'..callid..'1(args)'
	app.Set("CHANNEL(hangup_handler_push)=hangup-dial,"..cti_callid..",1(args)")

	if(not check_allow_external_num(external_num)) then
		app.Verbose("External number:"..external_num.." not allow!!!")
		return false
	end

	local gw_infos = get_gw_info(display_num)

	if(#(gw_infos) <= 0) then
		--app.Verbose("Can not find outbound gateway !!!")
		--return false

		app.Set('CALLERID(num)='..display_num)
		app.Dial('SIP/'..external_num..'@'..ENU_EXTERNAL,100,'TtU(answer^'..cti_callid..')')
		return true
	end

	local sel = 1
	local outbound_proxy = gw_infos[sel].name
	local ori_external_num = external_num

	if(string.sub(external_num,1,2) == '01') then
		external_num = string.sub(external_num,2)
	end

	if(string.sub(external_num,1,1) == '1') then
		local mobile_region = get_mobile_region(string.sub(external_num,1,7))
		if(mobile_region == false) then
			external_num = ori_external_num
		elseif (mobile_region ~= gw_infos[sel].area) then
			external_num = '0' .. external_num
		end
	end

	if(gw_infos[sel].type == 'nsg') then
		external_num = external_num..'-g=g1-h=a'
	end

	if(string.sub(display_num,1,4) == '0755') then
		if(gw_infos[sel].area == 'gd-sz') then
			display_num = string.sub(display_num,5)
		end
	end

	app.Set('CALLERID(num)='..display_num)
	app.Dial('SIP/'..external_num..'@'..outbound_proxy,100,'TtU(answer^'..cti_callid..')')

--[[
	app.Set('CALLERID(num)='..display_num)
	app.Dial('SIP/'..external_num..'@'..ENU_EXTERNAL,100,'TtU(answer^'..cti_callid..')')
]]

end


function set_exten_status(exten, status, cti_callid, callparam, local_ch, peer_exten, peer_ch, access_num)
	access_num = access_num or ''
	local old_status = get_exten_status(exten)
	redis_hset('tsr:'..exten, 'status', status)
	if (old_status ~= status) then
		local cdr_callid = redis_hget('call-context:'..cti_callid,'cdr_callid')
		local value = 'ex:'..exten..'|os:'..old_status..'|ns:'..status..'|tm:'..get_curtime()..'|tp:'..'1'..'|id:'..cdr_callid..'|pe:'..peer_exten..'|lch:'..local_ch..'|pch:'..peer_ch..'|cp:'..callparam..'|an:'..access_num..'|'
		redis_client:publish('status:exten', value)
	end
end


function dial_exten(cti_callid, exten, display_num)
	display_num = display_num or ''
	app.Verbose('dial_exten:'..cti_callid..':'..exten)
	local local_chan = channel['CHANNEL(name)']:get()
	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()
	local key = 'call-context:'..cti_callid

	local cti_callparam = redis_hget(key, 'callparam')
	local cti_type = redis_hget(key, 'type')
	local peernum = ''

	if (cti_type == 'callback') then
		local head = string.sub(cti_callid,1,1)
		if (head == '1') then
			-- nonthing.
		elseif(head == '2') then
			peernum = redis_hget(key, 'external_num')
		end
	elseif (cti_type == 'pv') then
		local head = string.sub(cti_callid,1,1)
		if (head== '1') then
			peernum = redis_hget(key,'callnum')
		elseif(head == '2') then
			peernum = redis_hget(key, 'exten')
		end
	else 
		if (string.sub(cti_type,1,7) == 'inbound') then
			set_exten_peer_channel(exten, local_chan)
		end
		peernum = channel['CALLERID(num)']:get()
	end

	local mode = redis_hget('tsr:'..exten, 'mode')
	if(mode == '1') then
		set_exten_status(exten, ES_RINGING, cti_callid, cti_callparam, ast_addr..'+'..local_chan, peernum, '')
		set_exten_ast_addr(exten, ast_addr)
		set_exten_channel(exten, local_chan)

		local map_num = redis_hget('tsr:'..exten, 'map_number')
		dial_external_num(cti_callid, display_num, map_num)
	else
		if (not exten_is_idle(exten)) then
			redis_client:del(key)
			app.Verbose('exten('..exten..') is not idle.')
			--channel['DIALSTATUS'] = 'BUSY'
			app.Set("DIALSTATUS=BUSY")
			return false
		end
		local opensips_addr = get_exten_opensips_addr(exten)
		if (opensips_addr == '') then
			redis_client:del(key)
			app.Verbose('[hget tsr:'..exten..' opensips] is nil.')
			--channel['DIALSTATUS'] = 'CHANUNAVAIL'
			app.Set("DIALSTATUS=CHANUNAVAIL")
			return false
		end

		--channel['CHANNEL(hangup_handler_push)'] = 'hangup-dial,'..cti_callid..'1(args)'
		app.Set("CHANNEL(hangup_handler_push)=hangup-dial,"..cti_callid..",1(args)")

		local normal_dial = false

		local is_kc_mode = redis_client:sismember('astkc:extens', exten)
		if (is_kc_mode == 1) then
			channel['localkc'] = '${IsLocalKC('..exten..')}'
			localkc = channel['localkc']:get()
			if (localkc ~= 0) then
				channel['localchannelname'] = '${GetLocalChanName('..exten..')}'
				app.Bridge(channel['localchannelname']:get())
			else
				normal_dial = true
			end
		else 
			normal_dial = true
		end

		if (normal_dial) then
			set_exten_status(exten, ES_RINGING, cti_callid, cti_callparam, ast_addr..'+'..local_chan, peernum, '')
			set_exten_ast_addr(exten, ast_addr)
			set_exten_channel(exten, local_chan)

			app.Dial('SIP/'..opensips_addr..'/'..exten,100,'TtU(answer^'..cti_callid..')')
		end
	end

	return true
end


function get_idle_exten(queue, wait, check_time, timeout)
	wait = wait or true
	check_time = check_time or 1
	timeout = timeout or 60
	local start_time = os.time()
	while true
	do
		local extens = redis_client:zrevrange('queue:'..queue..':tsr', '0', '-1')
		local k = nil
		local v = nil
		for k,v in pairs(extens) do
			if (exten_is_idle(v)) then
				set_queue_busy(queue,v)
				del_exten_from_queue(queue,v)
				del_queue_from_exten(v, queue)
				return v
			end
		end

		sleep(check_time)
		if ((os.time() - start_time) > timeout) then
			return ''
		end
	end
	return ''
end


function dial_queue(cti_callid, queue, display_num)
	display_num = display_num or ''
	local starttime = get_curtime()
	local key = 'call-context:'..cti_callid
	local cdr_callid = redis_hget(key,'cdr_callid')
	local record = redis_hget(key,'record')
	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()

	app.StartMusicOnHold()
	app.Verbose('Start get idle exten from ['..queue..'].')
	local exten = get_idle_exten(queue, 1, 20)
	app.Verbose('Get exten is ['..exten..'] from ['..queue..'].')

	redis_hset(key, 'exten', exten)

	local call_type = ''
	local caller = ''
	local caller_uniqueid = ''
	local callee = ''
	local callee_uniqueid = ''
	local did = ''
	local src_type = ''
	local src = ''

	local type = redis_hget(key,'type')

	if(type == 'callback') then
		call_type = CDR_CALL_TYPE_callback
		local callid = string.sub(cti_callid,2)
		local head = string.sub(cti_callid,1,1)
		if(head == '1') then
			local peer_cti_callid = '2'..callid
			redis_hset('call-context:'..peer_cti_callid, 'exten', exten)
		elseif(head == '2') then
			local peer_cti_callid = '1'..callid
			redis_hset('call-context:'..peer_cti_callid, 'exten', exten)

			caller = redis_hget(key,'external_num')
			callee = exten
			caller_uniqueid = ast_addr..'+'..redis_hget(key,'peer_channel')
			did = redis_hget(key,'display_num')
		end
	elseif(type == 'pv') then
		call_type = CDR_CALL_TYPE_pv
		local callid = string.sub(cti_callid,2) 
		local head = string.sub(cti_callid,1,1)
		if(head == '1') then
			--nonthing.
		elseif(head == '2') then
			--nonthing.
		end
	elseif(type == 'inbound-exten' or type == 'inbound-ivr-exten') then
		call_type = CDR_CALL_TYPE_inbound
		--nonthing.
	elseif(type == 'inbound-queue' or type == 'inbound-ivr-queue') then
		call_type = CDR_CALL_TYPE_inbound
		src_type = redis_hget(key, 'src_type')
		src = redis_hget(key, 'src')
		caller = redis_hget(key, 'callerid')
		caller_uniqueid = ast_addr..'+'..redis_hget(key, 'inbound_channel')
		callee = queue
		did = redis_hget(key, 'accessnum')
	elseif(type == 'inbound-external' or type == 'inbound-ivr-external') then
		call_type = CDR_CALL_TYPE_inbound
		--nonthing.
	elseif(type == 'spy') then
		call_type = CDR_CALL_TYPE_spy
		--nonthing.
	elseif(type == 'whisper') then
		call_type = CDR_CALL_TYPE_whisper
		--nonthing.
	elseif(type == 'transfer') then
		call_type = CDR_CALL_TYPE_transfer
		--nonthing.
	end

	local cdrdata = {}
	cdrdata['callid'] = cdr_callid
	cdrdata['call_type'] = call_type
	cdrdata['target_type'] = CDR_TARGET_TYPE_queue
	cdrdata['target'] = queue
	cdrdata['target_record'] = record
	cdrdata['target_groupid'] = get_queue_group(queue)
	cdrdata['src_type'] = src_type
	cdrdata['src'] = src
	cdrdata['caller'] = caller
	cdrdata['caller_uniqueid'] = caller_uniqueid
	cdrdata['callee'] = callee
	cdrdata['callee_uniqueid'] = callee_uniqueid
	cdrdata['did'] = ''
	cdrdata['starttime'] = starttime
	cdrdata['ringtime'] = ''
	cdrdata['answertime'] = starttime
	cdrdata['bridgetime'] = starttime
	cdrdata['endtime'] = get_curtime()
	cdrdata['call_cause'] = CDR_CALL_CAUSE_normal
	write_cdr(cdrdata)

	if (exten == '') then
		--channel['DIALSTATUS'] = 'BUSY'
		app.Set("DIALSTATUS=BUSY")
		--redis_client:del(key)
		app.Verbose('exten is empty #########################################################')
		return false
	else
		redis_hset(key, 'src_type', CDR_SRC_TYPE_queue)
		redis_hset(key, 'src', queue)
		redis_hset(key, 'starttime', get_curtime())

		return dial_exten(cti_callid, exten, display_num)
	end

	return false
end


function inbound_to_ivr(ivr_id, cti_callid, groupid)
	local ivr_servers = redis_client:zrange('ivr_servers', '0', '-1')

	if (ivr_servers[1] == nil) then
		app.Verbose('Can not found IVR Server.')
		return false
	end

	local ivr_server = redis_get( ivr_servers[1])
	if (ivr_server == '') then
		app.Verbose('Can not found IVR Server Address.')
		return false
	end

	app.AGI('agi://'..ivr_server..'/ivrid='..ivr_id..'&callid='..cti_callid..'&groupid='..groupid)

	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()
	local key = 'call-context:'..cti_callid
	local cdr_callid = redis_hget(key,'cdr_callid')
	local callerid = redis_hget(key, 'callerid')
	local accessnum = redis_hget(key, 'accessnum')
	local inbound_channel = redis_hget(key, 'inbound_channel')
	local starttime = redis_hget(key,'starttime')
	local answertime = redis_hget(key,'answertime')
	local bridgetime = redis_hget(key,'bridgetime')

	local cdrdata = {}
	cdrdata['callid'] = cdr_callid
	cdrdata['call_type'] = CDR_CALL_TYPE_inbound
	cdrdata['target_type'] = CDR_TARGET_TYPE_ivr
	cdrdata['target'] = ivr_id
	cdrdata['target_record'] = ''
	cdrdata['target_groupid'] = groupid
	cdrdata['src_type'] = ''
	cdrdata['src'] = ''
	cdrdata['caller'] = callerid
	cdrdata['caller_uniqueid'] = ast_addr..'+'..inbound_channel
	cdrdata['callee'] = ivr_id
	cdrdata['callee_uniqueid'] = ast_addr..'+'..inbound_channel
	cdrdata['did'] = accessnum
	cdrdata['starttime'] = starttime
	cdrdata['ringtime'] = ''
	cdrdata['answertime'] = answertime
	cdrdata['bridgetime'] = bridgetime
	cdrdata['endtime'] = get_curtime()
	cdrdata['call_cause'] = CDR_SRC_TYPE_none

	app.Verbose('Write IVR CDR #########################################')
	write_cdr(cdrdata)

	return true
end


function callback_first()
	local cdr_callid = channel['cti_callid']:get()
	local cti_callid = '1' .. channel['cti_callid']:get()
	local key = 'call-context:'..cti_callid

	local type = redis_hget(key, 'type')

	if(type ~= '') then
		app.Verbose('callid ['..cti_callid..'] already exist.')
		return false
	end

	local cti_display_num = channel['cti_display_num']:get()
	local cti_callparam = channel['cti_callparam']:get()
	cti_display_num = cti_display_num or ''
	cti_callparam = cti_callparam or ''


	redis_hset(key, 'type', 'callback')
	redis_hset(key, 'cdr_callid', cdr_callid)
	redis_hset(key, 'starttime', get_curtime())
	redis_hset(key, 'external_num', channel['cti_external_num']:get())
	redis_hset(key, 'display_num', cti_display_num)
	redis_hset(key, 'callparam', cti_callparam)
	redis_hset(key, 'queue', channel['cti_queue']:get())
	redis_hset(key, 'groupid', get_queue_group(channel['cti_queue']:get()))
	dial_external_num(cti_callid, cti_display_num, channel['cti_external_num']:get())
end


function callback_second()
	local cdr_callid = channel['cti_callid']:get()
	local cti_callid = '2' .. channel['cti_callid']:get()
	local key = 'call-context:'..cti_callid

	local cti_display_num = channel['cti_display_num']:get()
	local cti_callparam = channel['cti_callparam']:get()
	cti_display_num = cti_display_num or ''
	cti_callparam = cti_callparam or ''

	redis_hset(key, 'type', 'callback')
	redis_hset(key, 'cdr_callid', cdr_callid)
	redis_hset(key, 'starttime', get_curtime())
	redis_hset(key, 'queue', channel['cti_queue']:get())
	redis_hset(key, 'external_num', channel['cti_external_num']:get())
	redis_hset(key, 'display_num', cti_display_num)
	redis_hset(key, 'callparam', cti_callparam)
	redis_hset(key, 'groupid', get_queue_group(channel['cti_queue']:get()))

	if(not dial_queue(cti_callid, channel['cti_queue']:get(), cti_display_num)) then
		redis_client:del(key)
	end
end


function pv_first()

	local cdr_callid = channel['cti_callid']:get()
	local cti_callid = '1' .. channel['cti_callid']:get()
	local key = 'call-context:'..cti_callid

	local type = redis_hget(key, 'type')

	if( type ~= '') then
		app.Verbose('callid ['..cti_callid..'] already exist.')
		return false
	end

	local cti_display_num = channel['cti_display_num']:get()
	local cti_callparam = channel['cti_callparam']:get()
	cti_display_num = cti_display_num or ''
	cti_callparam = cti_callparam or ''

	redis_hset(key, 'type', 'pv')
	redis_hset(key, 'cdr_callid', cdr_callid)
	redis_hset(key, 'starttime', get_curtime())
	redis_hset(key, 'exten', channel['cti_exten']:get())
	redis_hset(key, 'calltype', channel['cti_calltype']:get())
	redis_hset(key, 'callnum', channel['cti_callnum']:get())
	redis_hset(key, 'display_num', cti_display_num)
	redis_hset(key, 'callparam', cti_callparam)
	redis_hset(key, 'groupid', get_exten_group(channel['cti_exten']:get()))

	dial_exten(cti_callid,channel['cti_exten']:get(),cti_display_num)
end


function pv_second()
	local cdr_callid = channel['cti_callid']:get()
	local cti_callid = '2' .. channel['cti_callid']:get()
	local key = 'call-context:'..cti_callid
	local cti_calltype = channel['cti_calltype']:get()

	local cti_display_num = channel['cti_display_num']:get()
	local cti_callparam = channel['cti_callparam']:get()
	cti_display_num = cti_display_num or ''
	cti_callparam = cti_callparam or ''

	redis_hset(key, 'type', 'pv')
	redis_hset(key, 'cdr_callid', cdr_callid)
	redis_hset(key, 'starttime', get_curtime())
	redis_hset(key, 'exten', channel['cti_exten']:get())
	redis_hset(key, 'calltype', cti_calltype)
	redis_hset(key, 'callnum', channel['cti_callnum']:get())
	redis_hset(key, 'display_num', cti_display_num)
	redis_hset(key, 'callparam', cti_callparam)
	redis_hset(key, 'groupid', get_exten_group(channel['cti_exten']:get()))

	if(cti_calltype == 'external') then
		dial_external_num(cti_callid, cti_display_num, channel['cti_callnum']:get())
	elseif(cti_calltype == 'exten') then
		dial_exten(cti_callid, channel['cti_callnum']:get(),cti_display_num)
	else
		app.Verbose('cti_calltype('..cti_calltype..') unknow')
		return false
	end
	return true
end


function answer()
	local local_chan = channel['CHANNEL(name)']:get()
	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()
	local cti_callid = channel['ARG1']:get()
	local answertime = get_curtime()
	local key = 'call-context:'..cti_callid

	redis_hset(key,'channel',local_chan)
	redis_hset(key, 'answertime', answertime)

	local type = redis_hget(key,'type')

	local head = ''
	local callid = ''

	if(type == 'callback' or type == 'pv') then
		head = string.sub(cti_callid,1,1)
		callid = string.sub(cti_callid,2)

		redis_hset('channel:'..ast_addr..'+'..local_chan, 'cdr_callid', callid)
	else
		redis_hset('channel:'..ast_addr..'+'..local_chan, 'cdr_callid', cti_callid)
	end

	redis_client:zincrby('asterisk_servers', '1', ast_addr)

	if(type == 'callback') then
		if(head == '1') then
			local peer_cti_callid = '2'..callid
			redis_hset('call-context:'..peer_cti_callid,'peer_channel',local_chan)
		elseif(head == '2') then
			local peer_cti_callid = '1'..callid
			local peer_key = 'call-context:'..peer_cti_callid
			local exten = redis_hget(key,'exten')
			local callparam = redis_hget(peer_key,'callparam')
			local callnum = redis_hget(peer_key,'callnum')
			local external_num = redis_hget(key,'external_num')
			local peer_chan = redis_hget(peer_key,'channel')
			redis_hset(key,'peer_channel',peer_chan)
			redis_hset(peer_key,'peer_channel',local_chan)
			redis_hset(key, 'bridgetime', answertime)
			redis_hset(peer_key, 'bridgetime', answertime)

			record_exten(cti_callid, exten)

			set_exten_status(external_num, ES_TALKING_OUT, cti_callid, callparam, ast_addr..'+'..peer_chan, exten, ast_addr..'+'..local_chan)

			set_exten_channel(exten, local_chan)
			set_exten_peer_channel(exten, peer_chan)
			set_exten_status(exten, ES_TALKING_IN, cti_callid, callparam, ast_addr..'+'..local_chan, external_num, ast_addr..'+'..peer_chan)
		end
	elseif(type == 'pv') then
		if(head == '1') then
			local peer_cti_callid = '2'..callid
			local peer_key = 'call-context:'..peer_cti_callid
			local exten = redis_hget(key,'exten')
			local callparam = redis_hget(key,'callparam')
			local calltype = redis_hget(key,'type')
			local callnum = redis_hget(key,'callnum')

			redis_hset(peer_key,'peer_channel',local_chan)

			set_exten_status(exten, ES_DIALING, cti_callid, callparam, ast_addr..'+'..local_chan, callnum, '')
			set_exten_channel(exten, local_chan)
		elseif(head == '2') then
			local peer_cti_callid = '1'..callid
			local peer_key = 'call-context:'..peer_cti_callid
			local exten = redis_hget(key,'exten')
			local callparam = redis_hget(key,'callparam')
			local calltype = redis_hget(key,'type')
			local callnum = redis_hget(key,'callnum')
			local peer_chan = redis_hget(peer_key,'channel')

			redis_hset(key,'peer_channel',peer_chan)
			redis_hset(peer_key,'peer_channel',local_chan)
			redis_hset(key, 'bridgetime', answertime)
			redis_hset(peer_key, 'bridgetime', answertime)

			record_exten(peer_cti_callid, exten)

			set_exten_channel(callnum, local_chan)
			set_exten_peer_channel(callnum, peer_chan)
			set_exten_status(callnum, ES_TALKING_IN, cti_callid, callparam, ast_addr..'+'..local_chan, exten, ast_addr..'+'..peer_chan)

			set_exten_peer_channel(exten, local_chan)
			set_exten_status(exten, ES_TALKING_OUT, cti_callid, callparam, ast_addr..'+'..peer_chan, callnum, ast_addr..'+'..local_chan)
		end
	elseif(type == 'inbound-exten' or type == 'inbound-ivr-exten') then
		local exten = redis_hget(key, 'exten')
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		redis_hset(key, 'bridgetime', answertime)

		record_exten(cti_callid, exten)

		set_exten_channel(exten, local_chan)
		set_exten_peer_channel(exten, inbound_channel)
		set_exten_status(exten, ES_TALKING_IN, cti_callid, '', ast_addr..'+'..local_chan, callerid, ast_addr..'+'..inbound_channel, accessnum)
	elseif(type == 'inbound-queue' or type == 'inbound-ivr-queue') then
		local queue = redis_hget(key, 'queue')
		local exten = redis_hget(key, 'exten')
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		redis_hset(key, 'bridgetime', answertime)

		record_exten(cti_callid, exten)

		set_exten_channel(exten, local_chan)
		set_exten_peer_channel(exten, inbound_channel)
		set_exten_status(exten, ES_TALKING_IN, cti_callid, '', ast_addr..'+'..local_chan, callerid, ast_addr..'+'..inbound_channel, accessnum)
	elseif(type == 'inbound-external' or type == 'inbound-ivr-external') then
		redis_hset(key, 'bridgetime', answertime)
	elseif(type == 'spy') then
		local spy = redis_hget(key, 'spy')
		local goal = redis_hget(key, 'goal')
		local goal_chan = ''
		if(goal ~= '') then
			goal_chan = get_exten_channel(goal)
			set_exten_peer_channel(goal, local_chan)
		else 
			goal_chan = redis_hget(key, 'goal_channel')
			goal = ''
		end
		redis_hset(key, 'bridgetime', answertime)

		record_exten(cti_callid, spy)

		set_exten_channel(spy, local_chan)
		set_exten_peer_channel(spy, goal_chan)
		set_exten_status(spy, ES_SPYING, '', '', ast_addr..'+'..local_chan, goal, goal_chan)
	elseif(type == 'whisper') then
		local whisper = redis_hget(key, 'whisper')
		local goal = redis_hget(key, 'goal')
		local goal_chan = ''
		if(goal ~= '') then
			goal_chan = get_exten_channel(goal)
			set_exten_peer_channel(goal, local_chan)
		else
			goal_chan = redis_hget(key, 'goal_channel')
			goal = ''
		end
		redis_hset(key, 'bridgetime', answertime)

		record_exten(cti_callid, whisper)

		set_exten_channel(whisper, local_chan)
		set_exten_peer_channel(whisper, goal_chan)
		set_exten_status(whisper, ES_WHISPER, '', '', ast_addr..'+'..local_chan, goal, goal_chan)
	elseif(type == 'transfer') then
		local old_channel = redis_hget(key, 'old_channel')
		redis_hset(key, 'bridgetime', answertime)
		local new = redis_hget(key, 'new')

		record_exten(cti_callid, new)

		set_exten_channel(new, local_chan)
		set_exten_peer_channel(new, old_channel)
		set_exten_status(new, ES_TALKING_IN, cti_callid, '', ast_addr..'+'..local_chan, '', old_channel)
	end
end


function hangup_dial(extension)
	local cti_callid = extension
	local key = 'call-context:'..cti_callid
	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()
	local type = redis_hget(key,'type')
	if(type == '') then
		app.Verbose('type is null.(channel is finish)')
		return true
	end

	local channel = redis_hget(key,'channel')
	if (channel ~= '') then
		app.Verbose('channel already exist.(do not anything)')
		return true
	end

	if(type == 'callback') then
		local callid = string.sub(cti_callid,2)
		local head = string.sub(cti_callid,1,1)
		if(head == '1') then
			local cdr_callid = redis_hget(key,'cdr_callid')
			local external_num = redis_hget(key,'external_num')
			local starttime = redis_hget(key,'starttime')
			local queue = redis_hget(key,'queue')
			local display_num = redis_hget(key,'display_num')
			local record = redis_hget(key,'record')
			local groupid = redis_hget(key,'groupid')

			local cdrdata = {}
			cdrdata['callid'] = cdr_callid
			cdrdata['call_type'] = CDR_CALL_TYPE_callback
			cdrdata['target_type'] = CDR_TARGET_TYPE_external
			cdrdata['target'] = external_num
			cdrdata['target_record'] = record
			cdrdata['target_groupid'] = groupid
			cdrdata['src_type'] = ''
			cdrdata['src'] = ''
			cdrdata['caller'] = external_num
			cdrdata['caller_uniqueid'] = ast_addr..'+'..channel
			cdrdata['callee'] = queue
			cdrdata['callee_uniqueid'] = ''
			cdrdata['did'] = display_num
			cdrdata['starttime'] = starttime
			cdrdata['ringtime'] = ''
			cdrdata['answertime'] = ''
			cdrdata['bridgetime'] = ''
			cdrdata['endtime'] = get_curtime()
			cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer
			write_cdr(cdrdata)
		elseif(head == '2') then
			local peer_cti_callid = '1'..callid
			local exten = redis_hget(key,'exten')
			local callparam = redis_hget('call-context:'..peer_cti_callid,'callparam')
			local external_num = redis_hget('call-context:'..peer_cti_callid,'external_num')

			set_exten_status(exten, ES_IDLE, cti_callid, callparam, '', external_num, '')
			set_exten_ast_addr(exten, '')
			set_exten_channel(exten, '')
			set_exten_peer_channel(exten, '')

			local cdr_callid = redis_hget(key,'cdr_callid')
			local external_num = redis_hget(key,'external_num')
			local starttime = redis_hget(key,'starttime')
			local peer_channel = redis_hget(key,'peer_channel')
			local queue = redis_hget(key,'queue')
			local exten = redis_hget(key,'exten')
			local external_num = redis_hget(key,'external_num')
			local display_num = redis_hget(key,'display_num')
			local record = redis_hget(key,'record')
			local groupid = redis_hget(key,'groupid')
			local src_type = redis_hget(key, 'src_type')
			local src = redis_hget(key, 'src')

			del_queue_busy(queue,exten)

			local cdrdata = {}
			cdrdata['callid'] = cdr_callid
			cdrdata['call_type'] = CDR_CALL_TYPE_callback
			cdrdata['target_type'] = CDR_TARGET_TYPE_exten
			cdrdata['target'] = exten
			cdrdata['target_record'] = record
			cdrdata['target_groupid'] = groupid
			cdrdata['src_type'] = src_type
			cdrdata['src'] = src
			cdrdata['caller'] = external_num
			cdrdata['caller_uniqueid'] = ast_addr..'+'..peer_channel
			cdrdata['callee'] = queue
			cdrdata['callee_uniqueid'] = ''
			cdrdata['did'] = display_num
			cdrdata['starttime'] = starttime
			cdrdata['ringtime'] = ''
			cdrdata['answertime'] = ''
			cdrdata['bridgetime'] = ''
			cdrdata['endtime'] = get_curtime()
			cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer
			write_cdr(cdrdata)
		end

	elseif(type == 'pv') then
		local callid = string.sub(cti_callid,2)
		local head = string.sub(cti_callid,1,1)
		if(head == '1') then
			local peer_cti_callid = '1'..callid
			local exten = redis_hget(key,'exten')
			local callparam = redis_hget('call-context:'..peer_cti_callid,'callparam')
			local callnum = redis_hget('call-context:'..peer_cti_callid,'callnum')

			set_exten_status(exten, ES_IDLE, cti_callid, callparam, '', '', '')
			set_exten_ast_addr(exten, '')
			set_exten_channel(exten, '')
			set_exten_peer_channel(exten, '')

			local cdr_callid = redis_hget(key,'cdr_callid')
			local external_num = redis_hget(key,'external_num')
			local starttime = redis_hget(key,'starttime')
			local exten = redis_hget(key,'exten')
			local external_num = redis_hget(key,'external_num')
			local display_num = redis_hget(key,'display_num')
			local record = redis_hget(key,'record')
			local groupid = redis_hget(key,'groupid')

			local cdrdata = {}
			cdrdata['callid'] = cdr_callid
			cdrdata['call_type'] = CDR_CALL_TYPE_pv
			cdrdata['target_type'] = CDR_TARGET_TYPE_exten
			cdrdata['target'] = exten
			cdrdata['target_record'] = record
			cdrdata['target_groupid'] = groupid
			cdrdata['src_type'] = ''
			cdrdata['src'] = ''
			cdrdata['caller'] = exten
			cdrdata['caller_uniqueid'] = ''
			cdrdata['callee'] = callnum
			cdrdata['callee_uniqueid'] = ''
			cdrdata['did'] = display_num
			cdrdata['starttime'] = starttime
			cdrdata['ringtime'] = ''
			cdrdata['answertime'] = ''
			cdrdata['bridgetime'] = ''
			cdrdata['endtime'] = get_curtime()
			cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer
			write_cdr(cdrdata)
		elseif(head == '2') then
			local calltype = redis_hget(key,'calltype')
			local target_type = ''
			local groupid = ''

			if(calltype == 'exten') then
				local peer_cti_callid = '1'..callid

				local exten = redis_hget(key,'exten')
				local callparam = redis_hget('call-context:'..peer_cti_callid,'callparam')
				local callnum = redis_hget(key,'callnum')

				set_exten_status(callnum, ES_IDLE, cti_callid, callparam, '', '', '')
				set_exten_ast_addr(callnum, '')
				set_exten_channel(callnum, '')
				set_exten_peer_channel(callnum, '')

				set_exten_status(exten, ES_IDLE, cti_callid, callparam, '', '', '')
				set_exten_ast_addr(exten, '')
				set_exten_channel(exten, '')
				set_exten_peer_channel(exten, '')

				target_type = CDR_TARGET_TYPE_exten
				groupid = get_exten_group(callnum)

			elseif(calltype == 'external') then
				target_type = CDR_TARGET_TYPE_external
				groupid = ''
			end

			local cdr_callid = redis_hget(key,'cdr_callid')
			local callnum = redis_hget(key,'callnum')
			local starttime = redis_hget(key,'starttime')
			local peer_channel = redis_hget(key,'peer_channel')
			local exten = redis_hget(key,'exten')
			local external_num = redis_hget(key,'external_num')
			local display_num = redis_hget(key,'display_num')
			local record = redis_hget(key,'record')
			local groupid = redis_hget(key,'groupid')

			local cdrdata = {}
			cdrdata['callid'] = cdr_callid
			cdrdata['call_type'] = CDR_CALL_TYPE_pv
			cdrdata['target_type'] = target_type
			cdrdata['target'] = callnum
			cdrdata['target_record'] = record
			cdrdata['target_groupid'] = groupid
			cdrdata['src_type'] = ''
			cdrdata['src'] = ''
			cdrdata['caller'] = exten
			cdrdata['caller_uniqueid'] = ast_addr..'+'..peer_channel
			cdrdata['callee'] = callnum
			cdrdata['callee_uniqueid'] = ''
			cdrdata['did'] = display_num
			cdrdata['starttime'] = starttime
			cdrdata['ringtime'] = ''
			cdrdata['answertime'] = ''
			cdrdata['bridgetime'] = ''
			cdrdata['endtime'] = get_curtime()
			cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer
			write_cdr(cdrdata)
		end
	elseif(type == 'inbound-ivr') then
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		local ivrid = redis_hget(key, 'ivr')
		local starttime = redis_hget(key,'starttime')
		local record = redis_hget(key,'record')
		local groupid = redis_hget(key,'groupid')

		local cdrdata = {}
		cdrdata['callid'] = cti_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_inbound
		cdrdata['target_type'] = CDR_TARGET_TYPE_ivr
		cdrdata['target'] = ivrid
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = callerid
		cdrdata['caller_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['callee'] = ivrid
		cdrdata['callee_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['did'] = accessnum
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = ''
		cdrdata['bridgetime'] = ''
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer

		write_cdr(cdrdata)
	elseif(type == 'inbound-exten' or type == 'inbound-ivr-exten') then
		local exten = redis_hget(key, 'exten')
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		local channel = redis_hget(key, 'channel')
		local record = redis_hget(key,'record')
		local starttime = redis_hget(key,'starttime')
		local groupid = redis_hget(key,'groupid')

		set_exten_status(exten, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(exten, '')
		set_exten_channel(exten, '')
		set_exten_peer_channel(exten, '')

		local cdrdata = {}
		cdrdata['callid'] = cti_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_inbound
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = exten
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = callerid
		cdrdata['caller_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['callee'] = exten
		cdrdata['callee_uniqueid'] = ast_addr..'+'..channel
		cdrdata['did'] = accessnum
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = ''
		cdrdata['bridgetime'] = ''
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer

		write_cdr(cdrdata)
	elseif(type == 'inbound-queue' or type == 'inbound-ivr-queue') then
		local starttime = redis_hget(key,'starttime')
		local queue = redis_hget(key, 'queue')
		local exten = redis_hget(key, 'exten')
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		local channel = redis_hget(key, 'channel')
		local record = redis_hget(key,'record')
		local groupid = redis_hget(key,'groupid')

		del_queue_busy(queue,exten)

		set_exten_status(exten, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(exten, '')
		set_exten_channel(exten, '')
		set_exten_peer_channel(exten, '')

		local cdrdata = {}
		cdrdata['callid'] = cti_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_inbound
		cdrdata['target_type'] = CDR_TARGET_TYPE_queue
		cdrdata['target'] = queue
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = callerid
		cdrdata['caller_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['callee'] = exten
		cdrdata['callee_uniqueid'] = ast_addr..'+'..channel
		cdrdata['did'] = accessnum
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = ''
		cdrdata['bridgetime'] = ''
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer

		write_cdr(cdrdata)

	elseif(type == 'spy') then
		local spy = redis_hget(key,'spy')

		set_exten_status(spy, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(spy, '')
		set_exten_channel(spy, '')
		set_exten_peer_channel(spy, '')

		local cdr_callid = redis_hget(key,'cdr_callid')
		local starttime = redis_hget(key,'starttime')
		local record = redis_hget(key,'record')
		local groupid = redis_hget(key,'groupid')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_spy
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = spy
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = ''
		cdrdata['caller_uniqueid'] = ''
		cdrdata['callee'] = ''
		cdrdata['callee_uniqueid'] = ''
		cdrdata['did'] = ''
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = ''
		cdrdata['bridgetime'] = ''
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer
		write_cdr(cdrdata)

	elseif(type == 'whisper') then
		whisper = redis_hget(key,'whisper')

		set_exten_status(whisper, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(whisper, '')
		set_exten_channel(whisper, '')
		set_exten_peer_channel(whisper, '')

		local cdr_callid = redis_hget(key,'cdr_callid')
		local starttime = redis_hget(key,'starttime')
		local record = redis_hget(key,'record')
		local groupid = redis_hget(key,'groupid')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_whisper
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = whisper
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = ''
		cdrdata['caller_uniqueid'] = ''
		cdrdata['callee'] = ''
		cdrdata['callee_uniqueid'] = ''
		cdrdata['did'] = ''
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = ''
		cdrdata['bridgetime'] = ''
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer
		write_cdr(cdrdata)

	elseif(type == 'transfer') then
		local new = redis_hget(key,'new')

		set_exten_status(new, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(new, '')
		set_exten_channel(new, '')
		set_exten_peer_channel(new, '')
		local record = redis_hget(key,'record')

		local cdr_callid = redis_hget(key,'cdr_callid')
		local starttime = redis_hget(key,'starttime')
		local groupid = redis_hget(key,'groupid')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_transfer
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = new
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = ''
		cdrdata['caller_uniqueid'] = ''
		cdrdata['callee'] = ''
		cdrdata['callee_uniqueid'] = ''
		cdrdata['did'] = ''
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = ''
		cdrdata['bridgetime'] = ''
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_target_no_answer
		write_cdr(cdrdata)
	end

	redis_client:del(key)

end

function hangup_answer(extension)
	local cti_callid = extension
	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()
	local key = 'call-context:'..cti_callid

	redis_client:zincrby('asterisk_servers', '-1', ast_addr)

	local type = redis_hget(key,'type')

	if(type == 'callback') then
		local callid = string.sub(cti_callid,2)
		local head = string.sub(cti_callid,1,1)
		if(head == '1') then
			local cdr_callid = redis_hget(key,'cdr_callid')
			local external_num = redis_hget(key,'external_num')
			local exten = redis_hget(key,'exten')
			local starttime = redis_hget(key,'starttime')
			local answertime = redis_hget(key,'answertime')
			local bridgetime = redis_hget(key,'bridgetime')
			local channel = redis_hget(key,'channel')
			local peer_channel = redis_hget(key,'peer_channel')
			local exten = redis_hget(key,'exten')
			local external_num = redis_hget(key,'external_num')
			local display_num = redis_hget(key,'display_num')
			local record = redis_hget(key,'record')
			local groupid = redis_hget(key,'groupid')

			local cdrdata = {}
			cdrdata['callid'] = cdr_callid
			cdrdata['call_type'] = CDR_CALL_TYPE_callback
			cdrdata['target_type'] = CDR_TARGET_TYPE_external
			cdrdata['target'] = external_num
			cdrdata['target_record'] = record
			cdrdata['target_groupid'] = groupid
			cdrdata['src_type'] = ''
			cdrdata['src'] = ''
			cdrdata['caller'] = external_num
			cdrdata['caller_uniqueid'] = ast_addr..'+'..channel
			cdrdata['callee'] = exten
			if(peer_channel ~= '') then
				cdrdata['callee_uniqueid'] = ast_addr .. '+' .. peer_channel
			else
				cdrdata['callee_uniqueid'] = ''
			end
			cdrdata['did'] = display_num
			cdrdata['starttime'] = starttime
			cdrdata['ringtime'] = ''
			cdrdata['answertime'] = answertime
			cdrdata['bridgetime'] = bridgetime
			cdrdata['endtime'] = get_curtime()
			cdrdata['call_cause'] = CDR_CALL_CAUSE_normal
			write_cdr(cdrdata)

		elseif(head == '2') then
			local channel = redis_hget(key,'channel')
			local peer_cti_callid = '1'..callid
			local peer_key = 'call-context:'..peer_cti_callid
			local exten = redis_hget(key,'exten')
			local callparam = redis_hget(peer_key,'callparam')
			local external_num = redis_hget(peer_key,'external_num')

			set_exten_status(exten, ES_IDLE, cti_callid, callparam, '', external_num, '')
			set_exten_ast_addr(exten, '')
			set_exten_channel(exten, '')
			set_exten_peer_channel(exten, '')

			local cdr_callid = redis_hget(key,'cdr_callid')
			local external_num = redis_hget(key,'external_num')
			local starttime = redis_hget(key,'starttime')
			local answertime = redis_hget(key,'answertime')
			local bridgetime = redis_hget(key,'bridgetime')
			local channel = redis_hget(key,'channel')
			local peer_channel = redis_hget(key,'peer_channel')
			local queue = redis_hget(key,'queue')
			local exten = redis_hget(key,'exten')
			local external_num = redis_hget(key,'external_num')
			local display_num = redis_hget(key,'display_num')
			local record = redis_hget(key,'record')
			local groupid = redis_hget(key,'groupid')
			local src_type = redis_hget(key, 'src_type')
			local src = redis_hget(key, 'src')

			del_queue_busy(queue,exten)

			local cdrdata = {}
			cdrdata['callid'] = cdr_callid
			cdrdata['call_type'] = CDR_CALL_TYPE_callback
			cdrdata['target_type'] = CDR_TARGET_TYPE_exten
			cdrdata['target'] = exten
			cdrdata['target_record'] = record
			cdrdata['target_groupid'] = groupid
			cdrdata['src_type'] = src_type
			cdrdata['src'] = src
			cdrdata['caller'] = external_num
			cdrdata['caller_uniqueid'] = ast_addr..'+'..peer_channel
			cdrdata['callee'] = exten
			cdrdata['callee_uniqueid'] = ast_addr..'+'..channel
			cdrdata['did'] = display_num
			cdrdata['starttime'] = starttime
			cdrdata['ringtime'] = ''
			cdrdata['answertime'] = answertime
			cdrdata['bridgetime'] = bridgetime
			cdrdata['endtime'] = get_curtime()
			cdrdata['call_cause'] = CDR_CALL_CAUSE_normal
			write_cdr(cdrdata)
		end
	elseif(type == 'pv') then
		local callid = string.sub(cti_callid,2)
		local head = string.sub(cti_callid,1,1)
		if(head == '1') then
			local exten = redis_hget(key,'exten')
			local calltype = redis_hget(key,'calltype')
			local callnum = redis_hget(key,'callnum')
			local display_num = redis_hset(key, 'display_num')
			local callparam = redis_hget(key,'callparam')

			set_exten_status(exten, ES_IDLE, cti_callid, callparam, '', '', '')
			set_exten_ast_addr(exten, '')
			set_exten_channel(exten, '')
			set_exten_peer_channel(exten, '')

			local cdr_callid = redis_hget(key,'cdr_callid')
			local external_num = redis_hget(key,'external_num')
			local starttime = redis_hget(key,'starttime')
			local answertime = redis_hget(key,'answertime')
			local bridgetime = redis_hget(key,'bridgetime')
			local channel = redis_hget(key,'channel')
			local peer_channel = redis_hget(key,'peer_channel')
			local exten = redis_hget(key,'exten')
			local external_num = redis_hget(key,'external_num')
			local display_num = redis_hget(key,'display_num')
			local record = redis_hget(key,'record')
			local groupid = redis_hget(key,'groupid')

			local cdrdata = {}
			cdrdata['callid'] = cdr_callid
			cdrdata['call_type'] = CDR_CALL_TYPE_pv
			cdrdata['target_type'] = CDR_TARGET_TYPE_exten
			cdrdata['target'] = exten
			cdrdata['target_record'] = record
			cdrdata['target_groupid'] = groupid
			cdrdata['src_type'] = ''
			cdrdata['src'] = ''
			cdrdata['caller'] = exten
			cdrdata['caller_uniqueid'] = ast_addr .. '+'.. channel
			cdrdata['callee'] = callnum
			if(peer_channel ~= '') then
				cdrdata['callee_uniqueid'] = ast_addr .. '+' .. peer_channel
			else
				cdrdata['callee_uniqueid'] = ''
			end
			cdrdata['did'] = display_num
			cdrdata['starttime'] = starttime
			cdrdata['ringtime'] = ''
			cdrdata['answertime'] = answertime
			cdrdata['bridgetime'] = bridgetime
			cdrdata['endtime'] = get_curtime()
			cdrdata['call_cause'] = CDR_CALL_CAUSE_normal

			write_cdr(cdrdata)

		elseif(head == '2') then
			local channel = redis_hget(key,'channel')
			local exten = redis_hget(key,'exten')
			local calltype = redis_hget(key,'calltype')
			local callnum = redis_hget(key,'callnum')
			local display_num = redis_hset(key, 'display_num')
			local callparam = redis_hget(key,'callparam')
			local target_type = ''

			if(calltype == 'exten') then
				set_exten_status(callnum, ES_IDLE, cti_callid, callparam, '', '', '')
				set_exten_ast_addr(callnum, '')
				set_exten_channel(callnum, '')
				set_exten_peer_channel(callnum, '')
				target_type = CDR_TARGET_TYPE_exten
			else
				target_type = CDR_TARGET_TYPE_external
			end

			local cdr_callid = redis_hget(key,'cdr_callid')
			local external_num = redis_hget(key,'external_num')
			local starttime = redis_hget(key,'starttime')
			local answertime = redis_hget(key,'answertime')
			local bridgetime = redis_hget(key,'bridgetime')
			local channel = redis_hget(key,'channel')
			local peer_channel = redis_hget(key,'peer_channel')
			local exten = redis_hget(key,'exten')
			local external_num = redis_hget(key,'external_num')
			local display_num = redis_hget(key,'display_num')
			local record = redis_hget(key,'record')
			local groupid = redis_hget(key,'groupid')

			local cdrdata = {}
			cdrdata['callid'] = cdr_callid
			cdrdata['call_type'] = CDR_CALL_TYPE_pv
			cdrdata['target_type'] = target_type
			cdrdata['target'] = callnum
			cdrdata['target_record'] = record
			cdrdata['target_groupid'] = groupid
			cdrdata['src_type'] = ''
			cdrdata['src'] = ''
			cdrdata['caller'] = exten
			cdrdata['caller_uniqueid'] = ast_addr .. '+' .. peer_channel
			cdrdata['callee'] = callnum
			cdrdata['callee_uniqueid'] = ast_addr .. '+' .. channel
			cdrdata['did'] = display_num
			cdrdata['starttime'] = starttime
			cdrdata['ringtime'] = ''
			cdrdata['answertime'] = answertime
			cdrdata['bridgetime'] = bridgetime
			cdrdata['endtime'] = get_curtime()
			cdrdata['call_cause'] = CDR_CALL_CAUSE_normal
			write_cdr(cdrdata)
		end

	elseif(type == 'inbound-external' or type == 'inbound-ivr-external') then
		local starttime = redis_hget(key,'starttime')
		local answertime = redis_hget(key,'answertime')
		local bridgetime = redis_hget(key,'bridgetime')
		local cdr_callid = redis_hget(key,'cdr_callid')
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		local channel = redis_hget(key, 'channel')
		local groupid = redis_hget(key,'groupid')
		local external_num = redis_hget(key,'external_num')
		local src_type = redis_hget(key,'src_type')
		local src = redis_hget(key,'src')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_inbound
		cdrdata['target_type'] = CDR_TARGET_TYPE_external
		cdrdata['target'] = external_num
		cdrdata['target_record'] = ''
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = src_type
		cdrdata['src'] = src

		cdrdata['caller'] = callerid
		cdrdata['caller_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['callee'] = exten
		cdrdata['callee_uniqueid'] = ast_addr..'+'..channel
		cdrdata['did'] = accessnum
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = answertime
		cdrdata['bridgetime'] = bridgetime
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_normal

		write_cdr(cdrdata)

	elseif(type == 'inbound-ivr') then
		local cdr_callid = redis_hget(key,'cdr_callid')
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		local ivrid = redis_hget(key, 'ivr')
		local starttime = redis_hget(key,'starttime')
		local answertime = redis_hget(key,'answertime')
		local bridgetime = redis_hget(key,'bridgetime')
		local groupid = redis_hget(key,'groupid')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_inbound
		cdrdata['target_type'] = CDR_TARGET_TYPE_external
		cdrdata['target'] = callerid
		cdrdata['target_record'] = ''
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = callerid
		cdrdata['caller_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['callee'] = ivrid
		cdrdata['callee_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['did'] = accessnum
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = answertime
		cdrdata['bridgetime'] = bridgetime
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_normal

		write_cdr(cdrdata)

	elseif(type == 'inbound-exten' or type == 'inbound-ivr-exten') then
		local starttime = redis_hget(key,'starttime')
		local answertime = redis_hget(key,'answertime')
		local bridgetime = redis_hget(key,'bridgetime')
		local cdr_callid = redis_hget(key,'cdr_callid')
		local exten = redis_hget(key, 'exten')
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		local channel = redis_hget(key, 'channel')
		local record = redis_hget(key,'record')
		local src_type = redis_hget(key,'src_type')
		local src = redis_hget(key,'src')
		local groupid = redis_hget(key,'groupid')

		set_exten_status(exten, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(exten, '')
		set_exten_channel(exten, '')
		set_exten_peer_channel(exten, '')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_inbound
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = exten
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = src_type
		cdrdata['src'] = src
		cdrdata['caller'] = callerid
		cdrdata['caller_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['callee'] = exten
		cdrdata['callee_uniqueid'] = ast_addr..'+'..channel
		cdrdata['did'] = accessnum
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = answertime
		cdrdata['bridgetime'] = bridgetime
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_normal

		write_cdr(cdrdata)
	elseif(type == 'inbound-queue' or type == 'inbound-ivr-queue') then
		local starttime = redis_hget(key,'starttime')
		local answertime = redis_hget(key,'answertime')
		local bridgetime = redis_hget(key,'bridgetime')
		local cdr_callid = redis_hget(key,'cdr_callid')
		local queue = redis_hget(key, 'queue')
		local exten = redis_hget(key, 'exten')
		local callerid = redis_hget(key, 'callerid')
		local accessnum = redis_hget(key, 'accessnum')
		local inbound_channel = redis_hget(key, 'inbound_channel')
		local channel = redis_hget(key, 'channel')
		local record = redis_hget(key,'record')
		local src_type = redis_hget(key,'src_type')
		local src = redis_hget(key,'src')
		local groupid = redis_hget(key,'groupid')

		del_queue_busy(queue,exten)

		set_exten_status(exten, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(exten, '')
		set_exten_channel(exten, '')
		set_exten_peer_channel(exten, '')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_inbound
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = exten
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = src_type
		cdrdata['src'] = src
		cdrdata['caller'] = callerid
		cdrdata['caller_uniqueid'] = ast_addr..'+'..inbound_channel
		cdrdata['callee'] = exten
		cdrdata['callee_uniqueid'] = ast_addr..'+'..channel
		cdrdata['did'] = accessnum
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = answertime
		cdrdata['bridgetime'] = bridgetime
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_normal

		write_cdr(cdrdata)
	elseif(type == 'spy') then
		local spy = redis_hget(key,'spy')

		set_exten_status(spy, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(spy, '')
		set_exten_channel(spy, '')
		set_exten_peer_channel(spy, '')

		local cdr_callid = redis_hget(key,'cdr_callid')
		local starttime = redis_hget(key,'starttime')
		local answertime = redis_hget(key,'answertime')
		local bridgetime = redis_hget(key,'bridgetime')
		local record = redis_hget(key,'record')
		local groupid = redis_hget(key,'groupid')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_spy
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = spy
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = ''
		cdrdata['caller_uniqueid'] = ''
		cdrdata['callee'] = ''
		cdrdata['callee_uniqueid'] = ''
		cdrdata['did'] = ''
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = answertime
		cdrdata['bridgetime'] = bridgetime
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_normal
		write_cdr(cdrdata)

	elseif(type == 'whisper') then
		local whisper = redis_hget(key,'whisper')

		set_exten_status(whisper, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(whisper, '')
		set_exten_channel(whisper, '')
		set_exten_peer_channel(whisper, '')

		local cdr_callid = redis_hget(key,'cdr_callid')
		local starttime = redis_hget(key,'starttime')
		local answertime = redis_hget(key,'answertime')
		local bridgetime = redis_hget(key,'bridgetime')
		local record = redis_hget(key,'record')
		local groupid = redis_hget(key,'groupid')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_whisper
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = whisper
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = ''
		cdrdata['caller_uniqueid'] = ''
		cdrdata['callee'] = ''
		cdrdata['callee_uniqueid'] = ''
		cdrdata['did'] = ''
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = answertime
		cdrdata['bridgetime'] = bridgetime
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_normal
		write_cdr(cdrdata)

	elseif(type == 'transfer') then
		local new = redis_hget(key,'new')

		set_exten_status(new, ES_IDLE, cti_callid, '', '', '', '')
		set_exten_ast_addr(new, '')
		set_exten_channel(new, '')
		set_exten_peer_channel(new, '')

		local cdr_callid = redis_hget(key,'cdr_callid')
		local starttime = redis_hget(key,'starttime')
		local answertime = redis_hget(key,'answertime')
		local bridgetime = redis_hget(key,'bridgetime')
		local record = redis_hget(key,'record')
		local groupid = redis_hget(key,'groupid')

		local cdrdata = {}
		cdrdata['callid'] = cdr_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_transfer
		cdrdata['target_type'] = CDR_TARGET_TYPE_exten
		cdrdata['target'] = new
		cdrdata['target_record'] = record
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = ''
		cdrdata['caller_uniqueid'] = ''
		cdrdata['callee'] = ''
		cdrdata['callee_uniqueid'] = ''
		cdrdata['did'] = ''
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = answertime
		cdrdata['bridgetime'] = bridgetime
		cdrdata['endtime'] = get_curtime()
		cdrdata['call_cause'] = CDR_CALL_CAUSE_normal
		write_cdr(cdrdata)
	end

	local channel = redis_hget(key,'channel')
	redis_client:del(key)
	redis_client:del('channel:'..ast_addr..channel)
end


function whisper()
	local cti_goal = channel['cti_goal']:get()
	local cti_goal_channel = ''
	if (cti_goal ~= '') then
		cti_goal_channel = get_exten_channel(cti_goal)
	else
		cti_goal_channel = channel['cti_goal_channel']:get()
	end

	app.ChanSpy(cti_goal_channel,'wqE')
end


function whisper_dial()
	local cti_callid = channel['cti_callid']:get()
	if (cti_callid == '') then
		app.Verbose('cti_callid not set')
		return false
	end

	local key = 'call-context:'..cti_callid

	local cti_whisper = channel['cti_whisper']:get()
	if (cti_whisper == '') then
		app.Verbose('cti_whisper not set')
		return false
	end

	local cti_goal = channel['cti_goal']:get()
	local cti_goal_channel = channel['cti_goal_channel']:get()
	cti_goal = cti_goal or ''
	cti_goal_channel = cti_goal_channel or ''

	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()

	if (cti_goal == '' and cti_goal_channel == '') then
		app.Verbose('cti_goal or cti_goal_channel not set')
		return false
	end

	redis_hset(key, 'type', 'whisper')
	redis_hset(key, 'whisper', cti_whisper)

	local tmp_chan = ''

	if(cti_goal ~= '') then
		redis_hset(key, 'goal', cti_goal)
		tmp_chan = get_exten_channel(cti_goal)
	end

	if(cti_goal_channel ~= '') then
		redis_hset(key, 'goal_channel', cti_goal_channel)
		tmp_chan = cti_goal_channel
	end

	cdr_callid = redis_hget('channel:'..ast_addr..tmp_chan,'cdr_callid')
	redis_hset(key,'cdr_callid',cdr_callid)
	redis_hset(key, 'starttime', get_curtime())
	redis_hset(key, 'groupid', get_exten_group(cti_whisper))

	local accessnum = redis_hget(key, 'accessnum')
	local display_num = ''
	if(accessnum ~= nil and accessnum ~= '') then
		display_num = accessnum
	else
		local num = redis_hget(key, 'display_num')
		if(num ~= nil and num ~= '') then
			display_num = num
		end
	end

	dial_exten(cti_callid, cti_whisper, display_num)
end


function spy()
	local cti_goal = channel['cti_goal']:get()
	if (cti_goal ~= '') then
		local cti_goal_channel = get_exten_channel(cti_goal)
	else
		local cti_goal_channel = channel['cti_goal_channel']:get()
	end

	app.ChanSpy(cti_goal_channel,'bqE')
end


function spy_dial()
	local cti_callid = channel['cti_callid']:get()
	if (cti_callid == '') then
		app.Verbose('cti_callid not set')
		return false
	end

	local cti_spy = channel['cti_spy']:get()
	if (cti_spy == '') then
		app.Verbose('cti_spy not set')
		return false
	end

	local key = 'call-context:'..cti_callid

	local cti_goal = channel['cti_goal']:get()
	local cti_goal_channel = channel['cti_goal_channel']:get()
	cti_goal = cti_goal or ''
	cti_goal_channel = cti_goal_channel or ''

	if (cti_goal == '' and cti_goal_channel == '') then
		app.Verbose('cti_goal or cti_goal_channel not set')
		return false
	end

	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()

	redis_hset(key, 'type', 'spy')
	redis_hset(key, 'spy', cti_spy)

	local tmp_chan = ''

	if(cti_goal ~= '') then
		redis_hset(key, 'goal', cti_goal)
		tmp_chan = get_exten_channel(cti_goal)
	end

	if(cti_goal_channel ~= '') then
		redis_hset(key, 'goal_channel', cti_goal_channel)
		tmp_chan = cti_goal_channel
	end

	cdr_callid = redis_hget('channel:'..ast_addr..'+'..tmp_chan, 'cdr_callid')

	redis_hset(key, 'cdr_callid', cti_callid)
	redis_hset(key, 'starttime', get_curtime())
	redis_hset(key, 'groupid', get_exten_group(cti_spy))

	local accessnum = redis_hget(key, 'accessnum')
	local display_num = ''
	if(accessnum ~= nil and accessnum ~= '') then
		display_num = accessnum
	else
		local num = redis_hget(key, 'display_num')
		if(num ~= nil and num ~= '') then
			display_num = num
		end
	end

	dial_exten(cti_callid, cti_spy, display_num)
end


function transfer_dial(extension)
	--format
	--cti-0-new-old-oldchannel
	--cti-1-new-oldchannel
	local exten = extension

	local transfer_info = string.split(exten,'-', 4)
	if (transfer_info == nil or transfer_info[4] == nil) then
		app.Verbose('transfer info failed')
		return false
	end

	local cti_new = transfer_info[3]
	local cti_old_channel = ''
	local cti_old = ''

	if (transfer_info[2] == '0') then
		local cti_old_and_channel = transfer_info[4];
		local old_and_channel = string.split(cti_old_and_channel,'-',2)
		if (old_and_channel == nil or old_and_channel[2] == nil) then
			app.Verbose('transfer info failed(2)')
			return false
		end
		cti_old = old_and_channel[1]
		cti_old_channel = old_and_channel[2]
	elseif (transfer_info[2] == '1') then
		cti_old_channel = transfer_info[4]
	end

	local cti_callid = make_callid(extension)
	local key = 'call-context:'..cti_callid

	if (cti_old == '' and cti_old_channel == '') then
		app.Verbose('cti_old and cti_old_channel not set')
		return false
	end

	if (cti_new == '') then
		app.Verbose('cti_new not set')
		return false
	end

	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()

	redis_hset(key, 'type', 'transfer')
	redis_hset(key, 'new', cti_new)
	redis_hset(key, 'starttime', get_curtime())
	redis_hset(key, 'groupid', get_exten_group(cti_new))

	if(cti_old ~= '') then
		redis_hset(key, 'old', cti_old)
	end

	redis_hset(key, 'old_channel', cti_old_channel)

	local cdr_callid = redis_hget('channel:'..ast_addr..'+'..cti_old_channel, 'cdr_callid')
	redis_hset(key,'cdr_callid',cdr_callid)

	local accessnum = redis_hget(key, 'accessnum')
	local display_num = ''
	if(accessnum ~= nil and accessnum ~= '') then
		display_num = accessnum
	else
		local num = redis_hget(key, 'display_num')
		if(num ~= nil and num ~= '') then
			display_num = num
		end
	end

	dial_exten( cti_callid, cti_new, display_num)
end


function inbound(extension)

	local prefix = channel['PREFIX']:get()
	if(prefix == nil) then
		prefix = ''
	end

	if(string.sub(extension,1,1) ~= 0) then
		extension = prefix .. extension
		channel['CALLERID(dnid)'] = extension
	end

	local accessnum = extension
	local callerid = channel['CALLERID(num)']:get()
	local local_chan = channel['CHANNEL(name)']:get()
	local ast_addr = channel['ENV(AST_SYSTEMNAME)']:get()

	if(string.sub(callerid,1,2) == '01') then
		callerid = string.sub(callerid,2)
	end

	-- Is local number.
	if(string.len(callerid) == 8) then
		callerid = prefix .. callerid
	end

	app.Verbose('accessnum:'..accessnum)
	app.Verbose('callerid:'..callerid) 

	local groupid = get_accessnum_group(accessnum)

	local cti_callid = make_callid(extension)
	local key = 'call-context:'..cti_callid

	local dest = ''

	if (callerid ~= '') then
		dest = redis_get( 'route:'..accessnum..':'..callerid)
		if (dest == '') then
			dest = redis_get( 'route:'..accessnum)
		end
	else
		dest = redis_get( 'route:'..accessnum)
	end

	local starttime = get_curtime()

	app.Verbose('dest:'..dest)

	local dest_ary = string.split(dest, '+')
	if (dest_ary == nil or dest_ary[2] == nil) then
		app.Verbose('not found router(dest='..dest..').')

		local cdrdata = {}
		cdrdata['callid'] = cti_callid
		cdrdata['call_type'] = CDR_CALL_TYPE_inbound
		cdrdata['target_type'] = CDR_TARGET_TYPE_external
		cdrdata['target'] = callerid
		cdrdata['target_record'] = ''
		cdrdata['target_groupid'] = groupid
		cdrdata['src_type'] = ''
		cdrdata['src'] = ''
		cdrdata['caller'] = callerid
		cdrdata['caller_uniqueid'] = ast_addr..'+'..local_chan
		cdrdata['callee'] = ''
		cdrdata['callee_uniqueid'] = ''
		cdrdata['did'] = accessnum
		cdrdata['starttime'] = starttime
		cdrdata['ringtime'] = ''
		cdrdata['answertime'] = ''
		cdrdata['bridgetime'] = ''
		cdrdata['endtime'] = starttime
		cdrdata['call_cause'] = CDR_CALL_CAUSE_no_route
		write_cdr(cdrdata)
		return false
	end

	app.Answer()

	redis_client:zincrby('asterisk_servers', '1', ast_addr)

	redis_hset(key, 'accessnum', accessnum)
	redis_hset(key, 'callerid', callerid)
	redis_hset(key, 'inbound_channel', local_chan)
	redis_hset(key, 'starttime', starttime)
	redis_hset(key, 'answertime', starttime)
	redis_hset(key, 'bridgetime', starttime)
	redis_hset(key, 'groupid', groupid)
	redis_hset(key, 'cdr_callid', cti_callid)

	--channel['CHANNEL(hangup_handler_push)'] = 'hangup-answer,'..cti_callid..'1(args)'
	app.Set("CHANNEL(hangup_handler_push)=hangup-answer,"..cti_callid..",1(args)")

	channel['CTI_CALLID'] = cti_callid

	app.Verbose('dest type:'..dest_ary[1])
	app.Verbose('dest number:'..dest_ary[2])

	if(dest_ary[1] == NT_EXTERNAL) then
		redis_hset(key,'type','inbound-external')
		local number = dest_ary[2]
		redis_hset(key,'external_num',number)
		dial_external_num( cti_callid, accessnum, number)
	elseif(dest_ary[1] == NT_EXTENSION) then
		redis_hset(key,'type','inbound-exten')
		local exten = dest_ary[2]
		redis_hset(key,'exten',exten)
		dial_exten(cti_callid, exten, accessnum)
	elseif(dest_ary[1] == NT_QUEUE) then
		redis_hset(key,'type','inbound-queue')
		local queue = dest_ary[2]
		redis_hset(key,'queue',queue)
		dial_queue(cti_callid, queue, accessnum)
	elseif(dest_ary[1] == NT_IVR) then
		redis_hset(key,'type','inbound-ivr')
		local ivr_id = dest_ary[2]
		redis_hset(key,'ivr',ivr_id)
		inbound_to_ivr(ivr_id, cti_callid, groupid)
	end
end


function ivr_to_dial(extension)
	local cti_callid = channel['CTI_CALLID']:get()
	app.Verbose('['..cti_callid..']')
	local key = 'call-context:'..cti_callid
	local accessnum = redis_hget(key, 'accessnum')
	local callerid = redis_hget(key, 'callerid')
	local ivrid = redis_hget(key, 'ivr')
	local inbound_channel = redis_hget(key, 'inbound_channel')

	local type_callnum = extension

	local type = string.sub(type_callnum,1,1)
	local callnum = string.sub(type_callnum,2)

	local new_cti_callid = make_callid(extension)
	local new_key = 'call-context:'..new_cti_callid

	redis_hset(new_key, 'cdr_callid', cti_callid)
	redis_hset(new_key, 'accessnum', accessnum)
	redis_hset(new_key, 'callerid', callerid)
	redis_hset(new_key, 'starttime', get_curtime())
	redis_hset(new_key, 'src_type', CDR_SRC_TYPE_ivr)
	redis_hset(new_key, 'src', ivrid)
	redis_hset(new_key, 'inbound_channel',inbound_channel)
	redis_hset(new_key, 'groupid',get_accessnum_group(accessnum))

	if(type == NT_EXTERNAL) then
		redis_hset(new_key, 'type', 'inbound-ivr-external')
		redis_hset(key,'external_num',callnum)
		dial_external_num( new_cti_callid, accessnum, callnum)
	elseif(type == NT_EXTENSION) then
		redis_hset(new_key, 'type', 'inbound-ivr-exten')
		redis_hset(new_key, 'exten', callnum)
		dial_exten(new_cti_callid, callnum, accessnum)
	elseif(type == NT_QUEUE) then
		redis_hset(new_key, 'type', 'inbound-ivr-queue')
		redis_hset(new_key, 'queue', callnum)
		dial_queue(new_cti_callid, callnum, accessnum)
	else
		redis_client:del(new_key)
	end
end


-- makecall
extensions = {}
extensions['makecall-first'] = {}
extensions['makecall-first']['callback'] = function(c,e)
	app.Verbose(c..':'..e) 
	callback_first()
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end

extensions['makecall-first']['pv'] = function(c,e)
	app.Verbose(c..':'..e)
	pv_first()
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end

extensions['makecall-second'] = {}
extensions['makecall-second']['callback'] = function(c,e)
	app.Verbose(c..':'..e) 
	callback_second()
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end

extensions['makecall-second']['pv'] = function(c,e)
	app.Verbose(c..':'..e) 
	pv_second()
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end


extensions['answer'] = {}
extensions['answer']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	--channel['CHANNEL(hangup_handler_push)'] = 'hangup-answer,'..channel['ARG1']:get()..'1(args)'
	app.Set("CHANNEL(hangup_handler_push)=hangup-answer,"..channel['ARG1']:get()..",1(args)")
	answer()
	app.Verbose('Exit '..c..':'..e)
	app.Return()
end


extensions['hangup-dial'] = {}
extensions['hangup-dial']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end

	hangup_dial(e)
	app.Verbose('Exit '..c..':'..e)
	app.Return()
end

extensions['hangup-answer'] = {}
extensions['hangup-answer']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	hangup_answer(e)
	app.Verbose('Exit '..c..':'..e)
	app.Return()
end


extensions['whisper'] = {}
extensions['whisper']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	whisper()
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end

extensions['whisper-dial'] = {}
extensions['whisper-dial']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	whisper_dial()
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end


extensions['spy'] = {}
extensions['spy']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	spy()
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end

extensions['spy-dial'] = {}
extensions['spy-dial']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	spy_dial()
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end


extensions['transfer-dial'] = {}
extensions['transfer-dial']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	transfer_dial(e)
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end


extensions['start-music-on-hold'] = {}
extensions['start-music-on-hold']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	app.StartMusicOnHold()
end

extensions['stop-music-on-hold'] = {}
extensions['stop-music-on-hold']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end	 
	app.StopMusicOnHold()
end


extensions['inbound'] = {}
extensions['inbound']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end
	inbound(e)
	app.Verbose('Exit '..c..':'..e)
	app.hangup()
end


extensions['ivr-to-dial'] = {}
extensions['ivr-to-dial']['_.'] = function(c,e)
	app.Verbose(c..':'..e)
	if(e == 'h') then
		return
	end	 
	ivr_to_dial(e)
	app.Verbose('Exit '..c..':'..e)
	app.Return()
end


