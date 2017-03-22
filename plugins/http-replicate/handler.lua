local ruunscope_serializer = require "kong.plugins.log-serializers.runscope"
local BasePlugin = require "kong.plugins.base_plugin"
local url = require "socket.url"

local HttpReplicateHandler = BasePlugin:extend()

HttpReplicateHandler.PRIORITY = 1

local HTTPS = "https"

local function generate_request_payload(request, parsed_url)
  if request["method"]:upper() == "GET" then
    return string.format(
      "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: Keep-Alive\r\nContent-Type: %s\r\nContent-Length: %s\r\n\r\n%s",
      request["method"]:upper(), parsed_url.path, parsed_url.host, request["headers"]["content-type"], 0, "")

  elseif request["method"]:upper() == "POST" then
    return string.format(
      "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: Keep-Alive\r\nContent-Type: %s\r\nContent-Length: %s\r\n\r\n%s",
      request["method"]:upper(), parsed_url.path, parsed_url.host, request["headers"]["content-type"], string.len(request["body"]), request["body"])

  elseif request["method"]:upper() == "PUT" then
    return string.format(
      "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: Keep-Alive\r\nContent-Type: %s\r\nContent-Length: %s\r\n\r\n%s",
      request["method"]:upper(), parsed_url.path, parsed_url.host, request["headers"]["content-type"], string.len(request["body"]), request["body"])

  elseif request["method"]:upper() == "PATCH" then
    return string.format(
      "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: Keep-Alive\r\nContent-Type: %s\r\nContent-Length: %s\r\n\r\n%s",
      request["method"]:upper(), parsed_url.path, parsed_url.host, request["headers"]["content-type"], string.len(request["body"]), request["body"])

  elseif request["method"]:upper() == "DELETE" then
    return string.format(
      "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: Keep-Alive\r\nContent-Type: %s\r\nContent-Length: %s\r\n\r\n%s",
      request["method"]:upper(), parsed_url.path, parsed_url.host, request["headers"]["content-type"], string.len(request["body"]), request["body"])
  end
end

-- Parse host url
-- @param `url`  host url
-- @return `parsed_url`  a table with host details like domain name, port, path etc
local function parse_url(host_url)
  local parsed_url = url.parse(host_url)
  if not parsed_url.port then
    if parsed_url.scheme == "http" then
      parsed_url.port = 80
    elseif parsed_url.scheme == HTTPS then
      parsed_url.port = 443
    end
  end
  if not parsed_url.path then
    parsed_url.path = "/"
  end
  return parsed_url
end

-- Log to a Http end point.
-- @param `premature`
-- @param `conf`     Configuration table, holds http endpoint details
-- @param `message`  Message to be logged
local function log(premature, conf, request, name)
  if premature then return end
  name = "["..name.."] "

  local ok, err
  local parsed_url = parse_url(conf.http_endpoint)
  local host = parsed_url.host
  local port = tonumber(parsed_url.port)

  local sock = ngx.socket.tcp()
  sock:settimeout(conf.timeout)

  ok, err = sock:connect(host, port)
  if not ok then
    ngx.log(ngx.ERR, name.."failed to connect to "..host..":"..tostring(port)..": ", err)
    return
  end

  if parsed_url.scheme == HTTPS then
    local _, err = sock:sslhandshake(true, host, false)
    if err then
      ngx.log(ngx.ERR, name.."failed to do SSL handshake with "..host..":"..tostring(port)..": ", err)
    end
  end

  ok, err = sock:send(generate_request_payload(request, parsed_url))
  if not ok then
    ngx.log(ngx.ERR, name.."failed to send data to "..host..":"..tostring(port)..": ", err)
  end

  ok, err = sock:setkeepalive(conf.keepalive)
  if not ok then
    ngx.log(ngx.ERR, name.."failed to keepalive to "..host..":"..tostring(port)..": ", err)
    return
  end
end

-- Only provide `name` when deriving from this class. Not when initializing an instance.
function HttpReplicateHandler:new(name)
  HttpReplicateHandler.super.new(self, name or "http-replicate")
end

function HttpReplicateHandler:access(conf)
  HttpReplicateHandler.super.log(self)

  ngx.req.read_body()
end

function HttpReplicateHandler:log(conf)
  HttpReplicateHandler.super.log(self)

  local request = ruunscope_serializer.serialize(ngx)["request"]
  request["body"] = ngx.req.get_body_data()

  local ok, err = ngx.timer.at(0, log, conf, request, self._name)
  if not ok then
    ngx.log(ngx.ERR, "["..self._name.."] failed to create timer: ", err)
  end
end

return HttpReplicateHandler
